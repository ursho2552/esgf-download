#!/usr/bin/env python3
"""
Generic ESGF (CMIP6, CMIP7, ...) downloader driven by a YAML or JSON config file.

The config lists which variables to get for which simulations, each given as
ESGF search facets. See the example configs next to this script; a minimal
CMIP6 one looks like

    variables:
      - {variable_id: tos, table_id: Oday}
    simulations:
      - {source_id: GFDL-ESM4, member_id: r1i1p1f1, experiments: [ssp245]}

and the CMIP7 equivalent like

    project: CMIP7
    layout: "{variable_id}_{frequency}/{source_id}/{experiment_id}/{filename}"
    variables:
      - {variable_id: tas, frequency: mon, region: glb, variable_branding_suffix: tavg-h2m-hxy-u}
    simulations:
      - {source_id: EC-Earth3-ESM-1-1, variant_label: r1i1p1f1, experiments: [esm-scen7-vl]}

Workflow
  1. Query several ESGF index nodes (file-level search, latest versions only,
     retracted files dropped) and merge all replicas of each file.
  2. Per simulation, experiment and variable keep one grid (by
     grid_preference) and the newest dataset version.
  3. Download each file from the first data node that works (preferred nodes
     first), verify its SHA256/MD5 checksum, and write a manifest.

Re-running is safe: complete files (size matches) are skipped, interrupted
downloads (*.part files) are resumed with HTTP Range requests, also within a
run when a connection drops.

Files written to the download directory
  <layout>          the data, default <variable>_<table>/<source>/<experiment>/<file>.nc
  manifest.csv      one line per selected file (grid, version, size, checksum, ...)
  missing.csv       requested combinations not found on ESGF
  search_errors.csv index node queries that failed (results may lack replicas)
  failed.csv        files that could not be downloaded in the last run

Only the Python standard library is needed for JSON configs; YAML configs need
PyYAML (available in the usual Python modules, e.g. `module load Python`).

Examples
  python3 esgf_download.py configs/cmip6_tos_thetao.yaml --dest /path/to/data --dry-run
  python3 esgf_download.py configs/cmip6_tos_thetao.yaml --dest /path/to/data
  python3 esgf_download.py configs/cmip6_tos_thetao.yaml --dest /path/to/data \\
      --models GFDL-ESM4 --variables tos --experiments ssp245
"""

import argparse
import csv
import hashlib
import json
import os
import random
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# Defaults, all of which can be overridden in the config file.
DEFAULTS = {
    "project": "CMIP6",
    # Legacy-API compatible search endpoints. They federate different subsets
    # of the data nodes (e.g. US originals only show up via the ORNL bridge),
    # so the results of all of them are merged.
    "index_nodes": [
        "https://esgf-data.dkrz.de/esg-search/search",
        "https://esgf.ceda.ac.uk/esg-search/search",
        "https://esgf-node.ornl.gov/esgf-1-5-bridge",
    ],
    # Data nodes tried first; all other replicas follow in random order.
    "preferred_data_nodes": [],
    "grid_preference": ["gn", "gr", "gr1", "gr2", "gr3"],
    "layout": "{variable_id}_{table_id}/{source_id}/{experiment_id}/{filename}",
    "workers": 4,
}

SEARCH_TIMEOUT = 300    # s
SEARCH_ATTEMPTS = 4     # per query, with growing pauses; failed queries get a second pass later
DOWNLOAD_TIMEOUT = 120  # s, per socket operation (not for the whole file)
MAX_STALLS = 3          # attempts without progress before trying the next replica
DEAD_NODE_FAILURES = 3  # failures without progress after which a node is tried last
CHUNK = 8 * 1024 * 1024
USER_AGENT = "esgf-download/1.0 (python-urllib)"


def log(msg):
    print(time.strftime("%H:%M:%S"), msg, flush=True)


def first(x):
    return x[0] if isinstance(x, list) else x


def as_list(x):
    return x if isinstance(x, list) else [x]


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
def load_config(path):
    with open(path) as f:
        if path.endswith((".yaml", ".yml")):
            try:
                import yaml
            except ImportError:
                sys.exit("YAML config needs PyYAML (module load Python, or pip install pyyaml); "
                         "alternatively use a JSON config.")
            cfg = yaml.safe_load(f)
        else:
            cfg = json.load(f)

    for key in ("variables", "simulations"):
        if not cfg.get(key):
            sys.exit(f"config {path}: '{key}' is missing or empty")
    for s in cfg["simulations"]:
        if not isinstance(s, dict) or not {"source_id", "experiments"} <= set(s):
            sys.exit(f"config {path}: each simulation needs source_id and experiments, got {s}")
    for v in [v for s in cfg["simulations"] for v in s.get("variables", [])] + cfg["variables"]:
        if not isinstance(v, dict) or "variable_id" not in v:
            sys.exit(f"config {path}: each variable needs a variable_id, got {v}")

    return {**DEFAULTS, **cfg}


# Keys of a simulation entry that are not search facets.
SIMULATION_KEYS = ("experiments", "variables")


def expand(facets):
    """All combinations of list-valued facets: {a: [1, 2], b: 3} -> [{a: 1, b: 3}, {a: 2, b: 3}].

    Lists are expanded into separate searches instead of being passed as
    repeated facet values, which some index nodes do not support.
    """
    combos = [{}]
    for key, value in facets.items():
        combos = [{**c, key: v} for c in combos for v in as_list(value)]
    return combos


def build_requests(cfg, models=None, experiments=None, variables=None):
    """Expand the config into a list of search requests.

    One request = (simulation facets, [experiments], variable facets), e.g.
    ({source_id: X, member_id: r1i1p1f1}, [ssp245], {variable_id: tos, table_id: Oday}).
    Facets are whatever the config gives, so any ESGF project works (CMIP6:
    member_id + table_id, CMIP7: variant_label + frequency/region/branding).
    A simulation may carry its own 'variables' list, replacing the global one.
    """
    reqs = []
    for sim in cfg["simulations"]:
        exps = [e for e in as_list(sim["experiments"]) if not experiments or e in experiments]
        if not exps:
            continue
        sim_facets = {k: v for k, v in sim.items() if k not in SIMULATION_KEYS}
        for sf in expand(sim_facets):
            if models and sf["source_id"] not in models:
                continue
            for var in sim.get("variables", cfg["variables"]):
                for vf in expand(var):
                    if variables and vf["variable_id"] not in variables:
                        continue
                    reqs.append((sf, exps, vf))
    return reqs


# --------------------------------------------------------------------------
# Search
# --------------------------------------------------------------------------
def search_files(index_url, facets, experiments):
    """Return all file records (all replicas) for the facets on one index node."""
    # One query per experiment: esg-search OR-s repeated facet values, but the
    # ORNL bridge silently keeps only the last one.
    return [d for e in experiments for d in search_experiment(index_url, facets, e)]


def search_experiment(index_url, facets, experiment):
    params = [("type", "File"), ("latest", "true"), ("format", "application/solr+json")]
    params += list(facets.items())
    params.append(("experiment_id", experiment))
    if "esg-search" in index_url:
        params.append(("distrib", "true"))

    docs, offset, limit = [], 0, 1000
    while True:
        query = urllib.parse.urlencode(params + [("limit", limit), ("offset", offset)])
        req = urllib.request.Request(f"{index_url}?{query}", headers={"User-Agent": USER_AGENT})
        for attempt in range(SEARCH_ATTEMPTS):
            try:
                with urllib.request.urlopen(req, timeout=SEARCH_TIMEOUT) as r:
                    resp = json.load(r)["response"]
                break
            except Exception as e:  # noqa: BLE001 (network errors come in many types)
                # A rejected query (e.g. a facet the node does not support)
                # would fail the same way again.
                rejected = (isinstance(e, urllib.error.HTTPError) and 400 <= e.code < 500
                            and e.code not in (408, 429))
                if rejected or attempt == SEARCH_ATTEMPTS - 1:
                    raise RuntimeError(f"search failed on {index_url}: {e}") from e
                time.sleep(15 * 2 ** attempt)
        docs += resp["docs"]
        offset += len(resp["docs"])
        if not resp["docs"] or offset >= resp["numFound"]:
            return docs


def http_urls(doc):
    """Plain HTTP(S) download URLs of a file record."""
    return [u.split("|")[0] for u in doc.get("url", []) if u.endswith("|HTTPServer")]


def collect(cfg, sim, experiments, var, index_nodes, out):
    """Merge file records over index nodes and replicas into out.

    out is {(experiment, dataset, grid, version): {filename: info}}, where
    dataset is the dataset id without grid and version. Returns
    {index_node: error} for the index nodes whose search failed.
    """
    facets = {"project": cfg["project"], **sim, **var}
    errors = {}
    for idx in index_nodes:
        try:
            docs = search_files(idx, facets, experiments)
        except RuntimeError as e:
            errors[idx] = str(e)
            continue
        for d in docs:
            if d.get("retracted") is True:
                continue
            # 'version' on file records is unreliable on some nodes; take it
            # from the dataset id: CMIP6.<...>.<grid>.v20190101|data_node
            # (CMIP7 ids end the same way).
            dataset, version = d["dataset_id"].split("|")[0].rsplit(".", 1)
            grid = first(d["grid_label"])
            # Datasets that differ only in grid and version are alternatives
            # to choose from. Anything else (e.g. two CMIP7 brandings of a
            # variable when the config does not fix the branding) is kept
            # as a separate dataset.
            dataset = dataset.removesuffix("." + grid)
            key = (first(d["experiment_id"]), dataset, grid, version)
            info = out[key].setdefault(d["title"], {
                "size": int(d.get("size", 0)),
                "checksum": first(d.get("checksum")),
                "checksum_type": first(d.get("checksum_type")),
                "urls": {},
                # all single-valued record fields, usable in the layout
                "fields": {k: first(v) for k, v in d.items() if isinstance(first(v), str)},
            })
            for u in http_urls(d):
                info["urls"].setdefault(u, d.get("data_node", ""))
    return errors


def select(records, experiments, grid_pref):
    """Pick one grid (by preference) and the newest version per dataset.

    Returns {experiment: [(dataset, grid, version, files), ...]}, normally
    one entry per experiment.
    """
    def rank(key):
        _, _, grid, version = key
        g_rank = grid_pref.index(grid) if grid in grid_pref else len(grid_pref)
        v = version.lstrip("v")
        return (g_rank, -int(v) if v.isdigit() else 0, grid)

    chosen = {}
    for exp in experiments:
        for dataset in sorted({k[1] for k in records if k[0] == exp}):
            best = min((k for k in records if k[:2] == (exp, dataset)), key=rank)
            chosen.setdefault(exp, []).append((dataset, best[2], best[3], records[best]))
    return chosen


# --------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------
# Data nodes that failed without delivering any data, shared by all download
# threads; such nodes are moved to the end of the list for later files.
NODE_FAILURES = defaultdict(int)
NODE_LOCK = threading.Lock()


def order_urls(urls, preferred_nodes):
    pref = {n: i for i, n in enumerate(preferred_nodes)}
    items = list(urls.items())
    random.shuffle(items)  # spread load over the non-preferred replicas
    with NODE_LOCK:
        failures = dict(NODE_FAILURES)
    items.sort(key=lambda kv: (failures.get(kv[1], 0) >= DEAD_NODE_FAILURES,
                               pref.get(kv[1], len(pref))))
    return items


def checksum_ok(path, ctype, expected):
    if not expected or not ctype:
        return True
    h = hashlib.new(ctype.lower().replace("-", ""))
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(CHUNK), b""):
            h.update(block)
    return h.hexdigest().lower() == expected.lower()


def fetch(url, part):
    """Append the rest of url to part, resuming with an HTTP Range request."""
    have = os.path.getsize(part) if os.path.exists(part) else 0
    headers = {"User-Agent": USER_AGENT}
    if have:
        headers["Range"] = f"bytes={have}-"
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=DOWNLOAD_TIMEOUT) as r:
        mode = "ab" if have and r.status == 206 else "wb"  # server may ignore Range
        with open(part, mode) as f:
            for block in iter(lambda: r.read(CHUNK), b""):
                f.write(block)


def download_one(job, preferred_nodes, verify_existing):
    target, info = job["target"], job["info"]
    if os.path.exists(target) and os.path.getsize(target) == info["size"]:
        if not verify_existing or checksum_ok(target, info["checksum_type"], info["checksum"]):
            return "skipped", target, None
    os.makedirs(os.path.dirname(target), exist_ok=True)
    # A *.part file left by an interrupted run is resumed. Replicas are
    # byte-identical, so it may be continued from any data node; the checksum
    # at the end catches anything inconsistent.
    part = target + ".part"
    name = os.path.basename(target)
    last_err = "no HTTP URL available"
    for url, node in order_urls(info["urls"], preferred_nodes):
        stalls = 0
        # Keep resuming from the same node as long as it makes progress
        # (slow far-away replicas often drop long connections).
        while True:
            before = os.path.getsize(part) if os.path.exists(part) else 0
            try:
                fetch(url, part)
            except Exception as e:  # noqa: BLE001 (network errors come in many types)
                last_err = f"{url}: {e}"
            else:
                size = os.path.getsize(part)
                if info["size"] and size > info["size"]:
                    last_err = f"{url}: got {size} B, expected {info['size']} B"
                    os.remove(part)
                    break
                if size == info["size"] or not info["size"]:
                    if checksum_ok(part, info["checksum_type"], info["checksum"]):
                        os.replace(part, target)
                        with NODE_LOCK:
                            NODE_FAILURES[node] = 0
                        return "ok", target, url
                    last_err = f"{url}: checksum mismatch"
                    os.remove(part)
                    break
                last_err = f"{url}: connection closed at {size} of {info['size']} B"
            after = os.path.getsize(part) if os.path.exists(part) else 0
            if after > before:
                stalls = 0
                log(f"resuming {name} at {human(after)} of {human(info['size'])} ({last_err})")
            else:
                stalls += 1
                # Failures are counted over all threads, so a node that is
                # offline (common) is given up on after a few attempts in
                # total instead of a few per file.
                with NODE_LOCK:
                    NODE_FAILURES[node] += 1
                    dead = NODE_FAILURES[node] >= DEAD_NODE_FAILURES
                if stalls >= MAX_STALLS or dead:
                    log(f"skip     {name} on {node}: {last_err}")
                    break
                log(f"retry    {name} ({stalls}/{MAX_STALLS}): {last_err}")
                time.sleep(10 * stalls)
    return "failed", target, last_err


def human(nbytes):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if nbytes < 1024 or unit == "TB":
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024


def write_csv(path, header, rows):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


# --------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("config", help="YAML (.yaml/.yml) or JSON config file")
    p.add_argument("--dest", help="download directory (overrides 'dest' in the config)")
    p.add_argument("--dry-run", action="store_true", help="only search and report files and sizes")
    p.add_argument("--workers", type=int, help="parallel downloads (overrides the config, default 4)")
    p.add_argument("--models", nargs="+", help="only these source_ids from the config")
    p.add_argument("--experiments", nargs="+", help="only these experiment_ids from the config")
    p.add_argument("--variables", nargs="+", help="only these variable_ids from the config")
    p.add_argument("--verify-existing", action="store_true",
                   help="also checksum files that already exist (slow)")
    args = p.parse_args()

    cfg = load_config(args.config)
    dest = args.dest or cfg.get("dest")
    if not dest:
        sys.exit("no download directory: pass --dest or set 'dest' in the config")
    dest = os.path.abspath(os.path.expanduser(dest))
    os.makedirs(dest, exist_ok=True)
    workers = args.workers or cfg["workers"]

    reqs = build_requests(cfg, args.models, args.experiments, args.variables)
    if not reqs:
        sys.exit("nothing to do: the filters removed all simulations/variables")

    # ---- search ----------------------------------------------------------
    # A failed index node can hide replicas that only it lists (e.g. US and
    # Japanese copies via the ORNL bridge), so failed queries are repeated
    # once more after all others and anything left is reported.
    results = []
    for sim, experiments, var in reqs:
        records = defaultdict(dict)
        errors = collect(cfg, sim, experiments, var, cfg["index_nodes"], records)
        for e in errors.values():
            log(f"WARNING {e} (will retry)")
        results.append((sim, experiments, var, records, errors))

    # CSV columns: the facets used in the config, e.g. source_id, member_id,
    # experiment_id, variable_id, table_id for CMIP6.
    sim_cols = list(dict.fromkeys(k for sim, _, _ in reqs for k in sim))
    var_cols = list(dict.fromkeys(k for _, _, var in reqs for k in var))

    def row(sim, experiment, var):
        return ([sim.get(k, "") for k in sim_cols] + [experiment]
                + [var.get(k, "") for k in var_cols])

    def tag(sim, experiment, var):
        return " ".join([*map(str, sim.values()), experiment, "/".join(map(str, var.values()))])

    search_errors = []
    retry = [r for r in results if r[4]]
    if retry:
        log(f"retrying {sum(len(r[4]) for r in retry)} failed searches")
    for sim, experiments, var, records, errors in retry:
        still = collect(cfg, sim, experiments, var, list(errors), records)
        for idx, e in still.items():
            log(f"WARNING {e}: {tag(sim, ' '.join(experiments), var)} may miss replicas")
            search_errors.append(row(sim, " ".join(experiments), var) + [idx, e])

    jobs, manifest, missing, paths = [], [], [], set()
    for sim, experiments, var, records, _ in results:
        chosen = select(records, experiments, cfg["grid_preference"])
        for exp in experiments:
            if exp not in chosen:
                log(f"MISSING {tag(sim, exp, var)}")
                missing.append(row(sim, exp, var))
                continue
            for dataset, grid, version, files in chosen[exp]:
                # several datasets when the config leaves a facet open
                which = f" ({dataset})" if len(chosen[exp]) > 1 else ""
                log(f"found   {tag(sim, exp, var)}: {grid} {version}, {len(files)} files, "
                    f"{human(sum(f['size'] for f in files.values()))}{which}")
                for fname, info in sorted(files.items()):
                    fields = {**info["fields"], "project": cfg["project"], **sim, **var,
                              "experiment_id": exp, "grid_label": grid, "version": version,
                              "filename": fname}
                    try:
                        rel = cfg["layout"].format(**fields)
                    except KeyError as e:
                        sys.exit(f"layout {cfg['layout']!r}: field {e} not known for {fname}; "
                                 f"set 'layout' in the config, available fields: "
                                 f"{', '.join(sorted(fields))}")
                    if rel in paths:
                        log(f"WARNING {rel} selected twice, skipped; add fields to 'layout' "
                            f"to tell the files apart")
                        continue
                    paths.add(rel)
                    jobs.append({"target": os.path.join(dest, rel), "info": info})
                    # facets the config leaves open are filled in from the file record
                    manifest.append(row(fields, exp, fields) + [
                        grid, version, fname, info["size"], info["checksum_type"],
                        info["checksum"], len(info["urls"]), rel])

    write_csv(os.path.join(dest, "manifest.csv"),
              sim_cols + ["experiment_id"] + var_cols + [
                  "grid_label", "version", "filename", "size_bytes", "checksum_type", "checksum",
                  "n_replicas", "path"],
              manifest)
    write_csv(os.path.join(dest, "missing.csv"), sim_cols + ["experiment_id"] + var_cols, missing)
    write_csv(os.path.join(dest, "search_errors.csv"),
              sim_cols + ["experiments"] + var_cols + ["index_node", "error"], search_errors)

    total = sum(j["info"]["size"] for j in jobs)
    log(f"{len(jobs)} files, {human(total)} in total; {len(missing)} combinations missing "
        f"(see {os.path.join(dest, 'missing.csv')})")
    if search_errors:
        log(f"WARNING {len(search_errors)} searches failed, results may be incomplete "
            f"(see {os.path.join(dest, 'search_errors.csv')}; re-run to retry)")
    if args.dry_run:
        return 0

    # ---- download --------------------------------------------------------
    n_ok = n_skip = 0
    failed = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(download_one, j, cfg["preferred_data_nodes"], args.verify_existing)
                   for j in jobs]
        for i, fut in enumerate(as_completed(futures), 1):
            status, target, detail = fut.result()
            rel = os.path.relpath(target, dest)
            if status == "ok":
                n_ok += 1
                log(f"[{i}/{len(jobs)}] ok      {rel}")
            elif status == "skipped":
                n_skip += 1
                log(f"[{i}/{len(jobs)}] exists  {rel}")
            else:
                failed.append([rel, detail])
                log(f"[{i}/{len(jobs)}] FAILED  {rel}  ({detail})")

    write_csv(os.path.join(dest, "failed.csv"), ["path", "last_error"], failed)
    log(f"done: {n_ok} downloaded, {n_skip} already present, {len(failed)} failed "
        f"(see {os.path.join(dest, 'failed.csv')}; re-run to retry)")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
