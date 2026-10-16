# CMIP6 / CMIP7 download from ESGF

`esgf_download.py` downloads climate model output (CMIP6, CMIP7) from ESGF. A small YAML (or
JSON) config file says which variables and which simulations you want; the
script finds the files, picks one grid and the newest version per dataset,
downloads them from whichever data node works, and checks every file's
checksum.

It needs only Python 3.9 or newer (standard library). YAML configs also need PyYAML.

```
esgf_download.py              the downloader
configs/cmip6_tos_thetao.yaml example: daily tos + monthly thetao for 19 models
configs/cmip7_example.yaml    example: CMIP7 config (see "CMIP7" below)
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
| `simulations` | required | List of facet sets, each with at least `source_id` and `experiments`. A simulation may have its own `variables:` list, which replaces the global one for that model. |
| `dest` | none | Download directory; `--dest` overrides it |
| `project` | `CMIP6` | ESGF project (`CMIP6`, `CMIP7`, ...) |
| `grid_preference` | `[gn, gr, gr1, gr2, gr3]` | If a dataset exists on several grids, the first one in this list is taken. Grids not in the list come after the listed ones. |
| `layout` | `{variable_id}_{table_id}/{source_id}/{experiment_id}/{filename}` | Path of each file below `dest`. Fields: any facet from the config, `project`, `experiment_id`, `grid_label`, `version`, `filename`, and any other field of the ESGF file record (e.g. `institution_id`, `activity_id`) |
| `workers` | `4` | Parallel downloads; `--workers` overrides it |
| `preferred_data_nodes` | `[]` | Data nodes tried first when a file has several copies (e.g. nodes close to you) |
| `index_nodes` | DKRZ, CEDA, ORNL bridge | ESGF search endpoints, all queried and merged |

Any other keys are ignored, so you can use them for YAML anchors, as the
example config does with `experiment_sets`.

A facet you leave out matches all of its values. If that matches several
datasets (e.g. two members, or two CMIP7 brandings of a variable), each one is
downloaded. The layout must then still give each file its own path. The
default layout does, because it ends in the CMIP file name; if two files would
still land on the same path, the second is skipped with a warning.

## CMIP7

The script works for CMIP7 with a config that uses CMIP7 facets; see
[configs/cmip7_example.yaml](configs/cmip7_example.yaml):

```yaml
project: CMIP7
grid_preference: []
layout: "{variable_id}_{frequency}/{source_id}/{experiment_id}/{filename}"
variables:
  - {variable_id: tos, frequency: mon, region: glb, variable_branding_suffix: tavg-u-hxy-sea}
simulations:
  - {source_id: EC-Earth3-ESM-1-1, variant_label: r1i1p1f1, experiments: [esm-scen7-vl]}
```

What differs from CMIP6:

| CMIP6 | CMIP7 |
|---|---|
| `table_id` (`Omon`, `Oday`, ...) | none; a variable is `variable_id` + `frequency` + `region` + `variable_branding_suffix` |
| `member_id` | `variant_label` |
| grids `gn`, `gr`, ... | grids like `g102`, `g114`: set `grid_preference` yourself |
| experiments `historical`, `ssp245`, ... | new names, e.g. `esm-scen7-vl` |

The default layout uses `table_id`, which CMIP7 does not have, so CMIP7
configs must set `layout`. Otherwise the script stops and lists the
fields it can use.

To find out which facet values exist, use the ESGF web search or a dataset
query, e.g.
<https://esgf-data.dkrz.de/esg-search/search?project=CMIP7&type=Dataset&distrib=true&format=application/solr%2Bjson&facets=source_id,experiment_id,variable_id,frequency,variable_branding_suffix&limit=0>.

As of October 2026 ESGF lists only two CMIP7 test datasets, and their
files are not yet searchable. So the CMIP7 support is tested only with
simulated search results built after the real dataset records.

## Command-line options

| Option | |
|---|---|
| `--dest DIR` | Download directory (overrides `dest` in the config) |
| `--dry-run` | Only search; write `manifest.csv`, `missing.csv` and `search_errors.csv` and report the total size |
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

The exit status is 0 if everything was downloaded and 1 if some files failed.

## How it works

1. **Search.** For each simulation, variable and experiment, every index node
   is queried for the latest, non-retracted files. The nodes list different
   copies (e.g. US and Japanese copies only appear via the ORNL bridge), so
   the results are merged. Failed queries are retried, once more at the end
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
