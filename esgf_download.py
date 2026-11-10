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

A simulation without source_id (or member) matches all models (members); with
--available such a config lists what exists instead of downloading it.

Workflow
  1. Query several ESGF index nodes (latest versions only, retracted files
     dropped) and merge all replicas of each file. CMIP6 is searched on the
     legacy ESGF search, CMIP7 on the STAC indices of ESGF-NG.
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
and with --available instead
  available.csv                one line per dataset found (all grids and matches)
  available_summary.csv        number of members per variable, model and experiment
  available_search_errors.csv  index node queries that failed

Only the Python standard library is needed for JSON configs; YAML configs need
PyYAML (available in the usual Python modules, e.g. `module load Python`).

Examples
  python3 esgf_download.py configs/cmip6_tos_thetao.yaml --dest /path/to/data --dry-run
  python3 esgf_download.py configs/cmip6_tos_thetao.yaml --dest /path/to/data
  python3 esgf_download.py configs/cmip6_tos_thetao.yaml --dest /path/to/data \\
      --models GFDL-ESM4 --variables tos --experiments ssp245
  python3 esgf_download.py configs/cmip7_search_tas_tos.yaml --available --dest /path/to/logs
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

# Search endpoints. Two kinds are supported, told apart by their URL:
#  - the legacy ESGF search (esg-search and the ORNL bridge), which holds
#    CMIP6 and earlier. The nodes federate different subsets of the data
#    nodes (e.g. US originals only show up via the ORNL bridge), so the
#    results of all of them are merged.
#  - the STAC discovery API of ESGF-NG, which holds CMIP7 (and copies of
#    CMIP6). East and West run different backends; both are merged too.
LEGACY_INDEX_NODES = [
    "https://esgf-data.dkrz.de/esg-search/search",
    "https://esgf.ceda.ac.uk/esg-search/search",
    "https://esgf-node.ornl.gov/esgf-1-5-bridge",
]
STAC_INDEX_NODES = [
    "https://discovery.east.esgf.io",
    "https://discovery.west.esgf.io",
]
# Default index_nodes per project; other projects use the legacy nodes.
DEFAULT_INDEX_NODES = {"CMIP7": STAC_INDEX_NODES}

# Defaults, all of which can be overridden in the config file.
DEFAULTS = {
    "project": "CMIP6",
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
    # A simulation without source_id (or member) matches all models (members),
    # e.g. to see with --available what exists for some experiments.
    for s in cfg["simulations"]:
        if not isinstance(s, dict) or "experiments" not in s:
            sys.exit(f"config {path}: each simulation needs experiments, got {s}")
    for v in [v for s in cfg["simulations"] for v in s.get("variables", [])] + cfg["variables"]:
        if not isinstance(v, dict) or "variable_id" not in v:
            sys.exit(f"config {path}: each variable needs a variable_id, got {v}")

    cfg = {**DEFAULTS, **cfg}
    cfg.setdefault("index_nodes", DEFAULT_INDEX_NODES.get(cfg["project"], LEGACY_INDEX_NODES))
    return cfg


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
        if models and "source_id" not in sim_facets:
            sim_facets = {"source_id": list(models), **sim_facets}
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
def is_stac(index_url):
    """True for an ESGF-NG STAC discovery API, False for the legacy search."""
    return "esg-search" not in index_url and "esgf-1-5-bridge" not in index_url


def search_files(index_url, project, facets, experiments):
    """Return all file records (all replicas) for the facets on one index node.

    Records are in the form of the legacy search; STAC results are converted.
    """
    search = search_stac if is_stac(index_url) else search_legacy
    # One query per experiment: esg-search OR-s repeated facet values, but the
    # ORNL bridge silently keeps only the last one.
    return [d for e in experiments for d in search(index_url, project, facets, e)]


def get_json(url, index_url, body=None):
    """GET url (or POST body as JSON) and return the decoded response, with retries."""
    headers = {"User-Agent": USER_AGENT}
    data = None
    if body is not None:
        headers["Content-Type"] = "application/json"
        data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers=headers)
    for attempt in range(SEARCH_ATTEMPTS):
        try:
            with urllib.request.urlopen(req, timeout=SEARCH_TIMEOUT) as r:
                return json.load(r)
        except Exception as e:  # noqa: BLE001 (network errors come in many types)
            # A rejected query (e.g. a facet the node does not support)
            # would fail the same way again.
            rejected = (isinstance(e, urllib.error.HTTPError) and 400 <= e.code < 500
                        and e.code not in (408, 429))
            if rejected or attempt == SEARCH_ATTEMPTS - 1:
                raise RuntimeError(f"search failed on {index_url}: {e}") from e
            time.sleep(15 * 2 ** attempt)


def search_legacy(index_url, project, facets, experiment):
    params = [("type", "File"), ("latest", "true"), ("format", "application/solr+json"),
              ("project", project)]
    params += list(facets.items())
    params.append(("experiment_id", experiment))
    if "esg-search" in index_url:
        params.append(("distrib", "true"))

    docs, offset, limit = [], 0, 1000
    while True:
        query = urllib.parse.urlencode(params + [("limit", limit), ("offset", offset)])
        resp = get_json(f"{index_url}?{query}", index_url)["response"]
        docs += resp["docs"]
        offset += len(resp["docs"])
        if not resp["docs"] or offset >= resp["numFound"]:
            return docs


# Collection ids per STAC index node (their case differs: CMIP7 vs cmip7).
STAC_COLLECTIONS = {}
# Multihash prefixes (hash function code + digest length) of ESGF-NG checksums.
MULTIHASH = {"1220": "SHA256", "1340": "SHA512", "d50110": "MD5"}


def stac_collection(index_url, project):
    if index_url not in STAC_COLLECTIONS:
        resp = get_json(f"{index_url}/collections", index_url)
        STAC_COLLECTIONS[index_url] = [c["id"] for c in resp["collections"]]
    for c in STAC_COLLECTIONS[index_url]:
        if c.lower() == project.lower():
            return c
    raise RuntimeError(f"search failed on {index_url}: no collection {project}")


def search_stac(index_url, project, facets, experiment):
    # Facets are item properties prefixed with the project (cmip7:source_id).
    # Only POST /search with a CQL2-JSON filter works on both East and West
    # (West ignores the filter of GET /items).
    prefix = project.lower() + ":"
    conds = [{"op": "=", "args": [{"property": "latest"}, True]}]
    conds += [{"op": "=", "args": [{"property": prefix + k}, v]}
              for k, v in {**facets, "experiment_id": experiment}.items()]
    body = {"collections": [stac_collection(index_url, project)], "limit": 100,
            "filter-lang": "cql2-json", "filter": {"op": "and", "args": conds}}
    url, docs = f"{index_url}/search", []
    while url:
        resp = get_json(url, index_url, body)
        for item in resp["features"]:
            docs += stac_files(item, prefix)
        nxt = next((ln for ln in resp.get("links", []) if ln.get("rel") == "next"), None)
        if not resp["features"] or not nxt:
            return docs
        url = nxt["href"]
        body = nxt.get("body") if nxt.get("method", "GET").upper() == "POST" else None
    return docs


def stac_files(item, prefix):
    """Legacy-style file records of a STAC item (one dataset on one data node)."""
    props = item["properties"]
    fields = {k.removeprefix(prefix): v for k, v in props.items()}
    docs = []
    for asset in item["assets"].values():
        # data files only, and only plain HTTP(S) (not Globus web links)
        if asset.get("protocol") not in ("http", "https") or "data" not in asset.get("roles", []):
            continue
        href = asset["href"]
        node = asset.get("alternate:name") or urllib.parse.urlparse(href).netloc
        checksum, ctype = asset.get("file:checksum"), None
        for code, name in MULTIHASH.items():
            if checksum and checksum.startswith(code):
                checksum, ctype = checksum[len(code):], name
                break
        docs.append({
            **fields,
            "title": os.path.basename(urllib.parse.urlparse(href).path),
            "dataset_id": f"{item['id']}|{node}",
            "data_node": node,
            "size": asset.get("file:size", 0),
            "checksum": checksum if ctype else None,
            "checksum_type": ctype,
            "url": [f"{href}|application/netcdf|HTTPServer"],
        })
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
    facets = {**sim, **var}
    errors = {}
    for idx in index_nodes:
        try:
            docs = search_files(idx, cfg["project"], facets, experiments)
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


# How datasets are described in available.csv and its summary, per project:
# identifying columns (taken from the file records), the ensemble member
# column, and the columns that together name a variable.
AVAILABLE = {
    "CMIP6": {
        "columns": ["activity_id", "institution_id", "source_id", "experiment_id", "member_id",
                    "table_id", "variable_id", "grid_label", "nominal_resolution"],
        "member": "member_id",
        "variable": ["variable_id", "table_id"],
    },
    "CMIP7": {
        "columns": ["activity_id", "institution_id", "source_id", "experiment_id", "variant_label",
                    "frequency", "region", "variable_id", "variable_branding_suffix", "grid_label",
                    "nominal_resolution"],
        "member": "variant_label",
        "variable": ["variable_id", "variable_branding_suffix", "frequency", "region"],
    },
}


def report_available(cfg, results, dest):
    """Write every dataset found (not only the selected ones) and a summary.

    available.csv          one line per dataset: facets, version, files, size, data nodes
    available_summary.csv  number of members per variable, model and experiment
    """
    spec = AVAILABLE.get(cfg["project"])
    if spec is None:  # other projects: the facets of the config
        facets = list(dict.fromkeys(k for sim, _, var, _, _ in results for k in {**sim, **var}))
        spec = {"columns": facets + ["experiment_id", "grid_label"], "member": "member_id",
                "variable": [k for k in facets if k != "source_id"]}
    rows, seen = [], set()
    for _, _, _, records, _ in results:
        for (exp, dataset, grid, version), files in sorted(records.items()):
            if (dataset, grid, version) in seen:
                continue
            seen.add((dataset, grid, version))
            fields = {**next(iter(files.values()))["fields"], "experiment_id": exp,
                      "grid_label": grid}
            nodes = sorted({n for f in files.values() for n in f["urls"].values()})
            rows.append([fields.get(c, "") for c in spec["columns"]] + [
                version, len(files), sum(f["size"] for f in files.values()), ";".join(nodes)])
    cols = spec["columns"] + ["version", "n_files", "size_bytes", "data_nodes"]
    write_csv(os.path.join(dest, "available.csv"), cols, rows)

    # summary: members per (variable, model) and experiment, experiments in config order
    ci = {c: i for i, c in enumerate(cols)}
    exps = list(dict.fromkeys(e for _, es, _, _, _ in results for e in es))
    members = defaultdict(set)
    size = defaultdict(int)
    mi = ci.get(spec["member"])
    for r in rows:
        var = "/".join(str(r[ci[c]]) for c in spec["variable"] if c in ci)
        # without a member column, count datasets
        members[(var, r[ci["source_id"]], r[ci["experiment_id"]])].add(
            r[mi] if mi is not None else tuple(r))
        size[var] += r[ci["size_bytes"]]
    pairs = sorted({k[:2] for k in members})
    summary = [[v, m] + [len(members.get((v, m, e), ())) for e in exps] for v, m in pairs]
    write_csv(os.path.join(dest, "available_summary.csv"), ["variable", "source_id"] + exps, summary)

    log(f"{len(rows)} datasets of {len({m for _, m in pairs})} models available; "
        f"number of members per variable, model and experiment ('-' = none):")
    # experiments without any data are named once instead of shown as empty columns
    shown = [i for i, e in enumerate(exps) if any(r[2 + i] for r in summary)]
    width = max([len("source_id")] + [len(m) for _, m in pairs])
    for var in sorted({v for v, _ in pairs}):
        print(f"\n{var}  ({human(size[var])} for all members)")
        print("  " + "source_id".ljust(width)
              + "".join(f"  {exps[i]:>{max(len(exps[i]), 3)}}" for i in shown))
        for v, m, *counts in summary:
            if v == var:
                print("  " + m.ljust(width) + "".join(
                    f"  {(str(counts[i]) if counts[i] else '-'):>{max(len(exps[i]), 3)}}"
                    for i in shown))
    empty = [e for i, e in enumerate(exps) if i not in shown]
    if empty:
        print(f"\nnothing available for: {', '.join(empty)}")
    print(flush=True)
    log(f"see {os.path.join(dest, 'available.csv')} and "
        f"{os.path.join(dest, 'available_summary.csv')}")


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
    p.add_argument("--available", action="store_true",
                   help="only search and list everything found (all grids, all matching "
                        "datasets) in available.csv and available_summary.csv; download "
                        "directory defaults to the current one")
    p.add_argument("--workers", type=int, help="parallel downloads (overrides the config, default 4)")
    p.add_argument("--models", nargs="+", help="only these source_ids from the config")
    p.add_argument("--experiments", nargs="+", help="only these experiment_ids from the config")
    p.add_argument("--variables", nargs="+", help="only these variable_ids from the config")
    p.add_argument("--verify-existing", action="store_true",
                   help="also checksum files that already exist (slow)")
    args = p.parse_args()

    cfg = load_config(args.config)
    dest = args.dest or cfg.get("dest") or ("." if args.available else None)
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
    errors_header = sim_cols + ["experiments"] + var_cols + ["index_node", "error"]

    if args.available:
        # separate files, so that a download directory's manifest stays untouched
        write_csv(os.path.join(dest, "available_search_errors.csv"), errors_header, search_errors)
        report_available(cfg, results, dest)
        if search_errors:
            log(f"WARNING {len(search_errors)} searches failed, the list may be incomplete "
                f"(see {os.path.join(dest, 'available_search_errors.csv')}; re-run to retry)")
        return 0

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
    write_csv(os.path.join(dest, "search_errors.csv"), errors_header, search_errors)

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
