# CMIP6 / CMIP7 download from ESGF

`esgf_download.py` downloads climate model output (CMIP6, CMIP7) from ESGF. A small YAML (or
JSON) config file says which variables and which simulations you want; the
script finds the files, picks one grid and the newest version per dataset,
downloads them from whichever data node works, and checks every file's
checksum.

It needs only Python 3.9 or newer (standard library). YAML configs also need PyYAML.

```
esgf_download.py                  the downloader
configs/cmip6_tos_thetao.yaml     CMIP6: daily tos + monthly thetao for 19 models
configs/cmip7_tas_tos.yaml        CMIP7: monthly tas + daily tos, emission-driven runs, 6 models
configs/cmip7_search_tas_tos.yaml CMIP7: list which tas/tos exists (see "Searching")
configs/cmip7_example.yaml        CMIP7: minimal example (one model, one experiment)
```

## Quick start

```bash
module load Python            # or any Python 3 with PyYAML

# 1. See what would be downloaded (files, sizes, missing datasets)
python3 esgf_download.py configs/cmip6_tos_thetao.yaml --dest /path/to/data --dry-run

# 2. Download
python3 esgf_download.py configs/cmip6_tos_thetao.yaml --dest /path/to/data
```

A full download can take days. Run it so that it survives logging out, and
keep the log:

```bash
nohup python3 esgf_download.py configs/cmip6_tos_thetao.yaml --dest /path/to/data \
    > download.log 2>&1 &
```

To fetch only part of a config, filter on the command line:

```bash
python3 esgf_download.py configs/cmip6_tos_thetao.yaml --dest /path/to/data \
    --models GFDL-ESM4 CESM2 --variables tos --experiments ssp245 ssp370
```

## Searching what is available

With `--available` the script only searches and lists everything it finds:
all models, members and grids that match the config, not only the ones it
would download. Leave `source_id` (and the member) out of the simulations
to match all models (and members):

```yaml
project: CMIP7
variables:
  - {variable_id: tos, frequency: [mon, day], region: glb, variable_branding_suffix: tavg-u-hxy-sea}
simulations:
  - {experiments: [esm-hist, esm-scen7-vl, esm-scen7-m, esm-scen7-h]}
```

```bash
python3 esgf_download.py configs/cmip7_search_tas_tos.yaml --available --dest /path/to/logs
```

This writes `available.csv`, with one line per dataset (model, member,
variable, grid, version, number of files, size, data nodes). It also writes
`available_summary.csv`, with the number of members per variable, model and
experiment, and prints that table:

```
tos/tavg-u-hxy-sea/day/glb  (2.5 TB for all members)
  source_id          historical  scen7-vl  scen7-m  scen7-h  esm-hist  esm-scen7-vl  ...
  ACCESS-ESM1-6              10         -        -        -        30             -
  CanESM6-0-MR               25        25       25       25        25            25
  ...
```

These files have their own names, so running `--available` in a download
directory leaves its `manifest.csv` alone. Without `--dest`, they go to the
current directory. `--models`, `--experiments` and `--variables` narrow the
search as usual.

## Writing a config

A minimal config:

```yaml
variables:
  - {variable_id: tos, table_id: Oday}
simulations:
  - {source_id: GFDL-ESM4, member_id: r1i1p1f1, experiments: [historical, ssp245]}
```

Every variable is downloaded for every simulation. The entries in
`variables` and `simulations` are ESGF search facets: every key except
`experiments` (and a simulation's own `variables`) is passed to the search
as is. So you can use whatever facets the project has, e.g. `member_id` and
`table_id` for CMIP6, or `variant_label` and `frequency` for CMIP7. A facet
given as a list (e.g. `member_id: [r1i1p1f1, r2i1p1f1]`) is searched for
each value.

The full set of keys:

| Key | Default | Meaning |
|---|---|---|
| `variables` | required | List of facet sets, each with at least `variable_id` |
| `simulations` | required | List of facet sets, each with at least `experiments`, normally also `source_id` and the member. A simulation may have its own `variables:` list, which replaces the global one for that model. |
| `dest` | none | Download directory; `--dest` overrides it |
| `project` | `CMIP6` | ESGF project (`CMIP6`, `CMIP7`, ...) |
| `grid_preference` | `[gn, gr, gr1, gr2, gr3]` | If a dataset exists on several grids, the first one in this list is taken. Grids not in the list come after the listed ones. |
| `layout` | `{variable_id}_{table_id}/{source_id}/{experiment_id}/{filename}` | Path of each file below `dest`. Fields: any facet from the config, `project`, `experiment_id`, `grid_label`, `version`, `filename`, and any other field of the ESGF file record (e.g. `institution_id`, `activity_id`) |
| `workers` | `4` | Parallel downloads; `--workers` overrides it |
| `preferred_data_nodes` | `[]` | Data nodes tried first when a file has several copies (e.g. nodes close to you) |
| `index_nodes` | CMIP7: ESGF-NG East and West; others: DKRZ, CEDA, ORNL bridge | ESGF search endpoints, all queried and merged. URLs containing `esg-search` or `esgf-1-5-bridge` are the legacy search; any other URL is taken as a STAC API (e.g. `https://discovery.east.esgf.io`). |

Any other keys are ignored, so you can use them for YAML anchors, as the
example config does with `experiment_sets`.

A facet you leave out matches all of its values. If that matches several
datasets (e.g. two members, or two CMIP7 brandings of a variable), each one is
downloaded. The layout must then still give each file its own path. The
default layout does, because it ends in the CMIP file name; if two files would
still land on the same path, the second is skipped with a warning.

## CMIP7

CMIP7 is not in the legacy ESGF search that holds CMIP6. It is published in
ESGF-NG, whose two catalogues (East and West) have a STAC API. With
`project: CMIP7`, the script searches these by default.
Everything else (copies, resuming, checksums) works as for CMIP6.

A CMIP7 config uses CMIP7 facets; see
[configs/cmip7_tas_tos.yaml](configs/cmip7_tas_tos.yaml):

```yaml
project: CMIP7
grid_preference: []
layout: "{variable_id}_{frequency}/{source_id}/{experiment_id}/{filename}"
variables:
  - {variable_id: tas, frequency: mon, region: glb, variable_branding_suffix: tavg-h2m-hxy-u}
  - {variable_id: tos, frequency: day, region: glb, variable_branding_suffix: tavg-u-hxy-sea}
simulations:
  - {source_id: CanESM6-0-MR, variant_label: r1i1p1f1,
     experiments: [esm-hist, esm-scen7-vl, esm-scen7-m, esm-scen7-h]}
```

What differs from CMIP6:

| CMIP6 | CMIP7 |
|---|---|
| `table_id` (`Omon`, `Oday`, ...) | none; a variable is `variable_id` + `frequency` + `region` + `variable_branding_suffix` |
| `member_id` | `variant_label` |
| grids `gn`, `gr`, ... | grids like `g102`, `g114`: set `grid_preference` yourself |
| experiments `historical`, `ssp245`, ... | concentration-driven `historical`, `scen7-vl`, `scen7-m`, `scen7-h`, ...; emission-driven `esm-hist`, `esm-scen7-vl`, ... |

The default layout uses `table_id`, which CMIP7 does not have, so CMIP7
configs must set `layout`. Otherwise the script stops and lists the
fields it can use.

To find out which models, members and variables exist, use `--available`
(see "Searching") or the ESGF web search at
<https://metagrid.esgf-west.org/search>.

As of 10 November 2026, the CMIP7 data on ESGF covers 11 models with `tas`
or `tos` for historical or scenario runs. Only the `vl`, `m` and `h`
scenarios have been published so far.

## Command-line options

| Option | |
|---|---|
| `--dest DIR` | Download directory (overrides `dest` in the config) |
| `--dry-run` | Only search; write `manifest.csv`, `missing.csv` and `search_errors.csv` and report the total size |
| `--available` | Only search and list everything found in `available.csv` and `available_summary.csv` (see "Searching") |
| `--workers N` | Parallel downloads |
| `--models`, `--experiments`, `--variables` | Only these entries of the config |
| `--verify-existing` | Also recompute the checksum of files that are already there (slow) |

## What you get

In the download directory:

| File | Contents |
|---|---|
| `<layout>` | The NetCDF files |
| `manifest.csv` | One line per selected file: the facets of the config (model, member, experiment, variable, ...), grid, version, size, checksum, number of copies found, path |
| `missing.csv` | Requested combinations of facets and experiment that ESGF does not have |
| `search_errors.csv` | Search queries that failed; datasets listed here may be missing copies or files |
| `failed.csv` | Files that could not be downloaded in the last run, with the last error |

With `--available`, `available.csv`, `available_summary.csv` and
`available_search_errors.csv` are written instead.

The exit status is 0 if everything was downloaded and 1 if some files failed.

## How it works

1. **Search.** For each simulation, variable and experiment, every index node
   is queried for the latest, non-retracted files: the legacy ESGF search
   for CMIP6, the ESGF-NG STAC API for CMIP7. The nodes list different
   copies (e.g. US and Japanese CMIP6 copies only appear via the ORNL
   bridge), so the results are merged. Failed queries are retried, once more at the end
   of the search, and anything still failing goes to `search_errors.csv`.
2. **Select.** Per dataset (model, member, experiment, variable, ...), one
   grid (by `grid_preference`) and the newest dataset version are kept.
3. **Download.** Each file is fetched over HTTP from its copies in turn:
   preferred nodes first, the rest in random order. Broken connections are
   resumed where they stopped. A data node that keeps failing without
   delivering data is given up on and tried last for the remaining files.
   Every file is checked against its SHA256/MD5 checksum before it is kept.

## Re-running

Re-running the same command is always safe and is how you retry failures:

- Files that are already complete (right size) are skipped.
- Partly downloaded files (`*.part`) are resumed.
- The search runs again, so new versions or newly available copies are
  picked up.

## Troubleshooting

**Files keep failing with `timed out` on one data node.** Look up the file in
`manifest.csv`: if `n_replicas` is 1, that node is the only source and the
script cannot do anything until it is back online. Re-run later. If a node is
down for good, remove it from `preferred_data_nodes` so it is not tried first.

**A dataset is in `missing.csv` but you know it exists.** Check
`search_errors.csv`: a failed search on one index node can hide datasets
that only that node lists. Re-run, or try the search later.

**Downloads from some nodes are very slow.** Far-away nodes (US, Japan) can
deliver below 1 MB/s per file. More `--workers` helps the total rate.

**Checksum mismatch.** The file is deleted and the next copy is tried. If all
copies fail, the file ends up in `failed.csv`.
