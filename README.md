# pipeline-import

A small, dependency-free Python CLI that reads CI/CD pipeline run rows from a CSV
file and upserts them into [DX](https://getdx.com) (Datacloud) via the
`pipelineRuns.upsert` API.

It was originally built around TeamCity exports, but works with CSV exports
from any CI/CD system — you'll be asked which system the data came from (or
you can set it per-row, see below).

## Requirements

- Python 3.10+ (uses `from __future__ import annotations` and `X | None` typing)
- No third-party packages — it relies only on the Python standard library.

## Setup

1. Copy the example environment file and fill in your credentials:

```bash
cp .env.example .env
```

2. Edit `.env`:

```dotenv
DATACLOUD_API_TOKEN=your-api-token-here
DATACLOUD_BASE_URL=https://your-instance.getdx.net
```

The `.env` file and any `*.csv` files are git-ignored so secrets and data are
never committed.

## CSV format

The first row must be a header. Recognized columns (all optional except those
your DX instance requires) are:

| Column            | Description                                  |
| ----------------- | -------------------------------------------- |
| `reference_id`    | Unique ID for the pipeline run (upsert key)  |
| `pipeline_name`   | Name of the pipeline / build configuration   |
| `status`          | e.g. `success`, `failed`                     |
| `started_at`      | ISO 8601 timestamp                           |
| `finished_at`     | ISO 8601 timestamp                           |
| `repository`      | `org/repo`                                    |
| `commit_sha`      | Commit hash                                  |
| `head_branch`     | Branch name                                  |
| `pr_number`       | Pull request number                          |
| `email`           | Author email                                 |
| `github_username` | Author GitHub handle                         |
| `gitlab_username` | Author GitLab handle                         |
| `source_url`      | Link back to the build in the CI system      |
| `source`          | CI/CD system for this row (e.g. `Jenkins`, `TeamCity`, `GitHub Actions`) |

Empty values are dropped from each payload. `pipeline_source` is set from
each row's `source` column when present; otherwise it falls back to
`--source`. If `--source` isn't provided, you'll be prompted for a default
at startup — this lets you import CSVs from a single CI/CD system without
adding a `source` column to every row. If you're importing rows from
multiple CI/CD systems in one CSV, add a `source` column and set it per row
instead — that value takes precedence over the default for that row.

See `sample_teamcity_pipeline_runs.csv` for a complete example, or
`data/example_3_multi_source.csv` for a CSV with a per-row `source` column.

## Usage

### Drop-in directory (easiest)

Drop any number of `*.csv` files into the `data/` directory, then run with no
arguments. All files are discovered, sorted, and processed first-to-last:

```bash
python main.py
```

The drop-in directory defaults to `./data` and can be changed with `--csv-dir`
or the `DATACLOUD_CSV_DIR` environment variable.

### Explicit files and directories

You can also pass one or more CSV files and/or directories. Directories are
scanned for `*.csv`:

```bash
python main.py runs-q1.csv runs-q2.csv
python main.py ./exports                       # every *.csv in ./exports
python main.py sample_teamcity_pipeline_runs.csv ./exports
```

Preview payloads without sending anything:

```bash
python main.py --dry-run sample_teamcity_pipeline_runs.csv
```

### Options

| Flag          | Description                                                          |
| ------------- | ------------------------------------------------------------------- |
| `CSV ...`     | Zero or more CSV files/directories. Defaults to the drop-in dir.    |
| `--csv-dir`   | Drop-in directory scanned for `*.csv` (overrides `DATACLOUD_CSV_DIR`). |
| `--env-file`  | Path to the `.env` file. Defaults to `.env`.                        |
| `--base-url`  | DX base URL (overrides `DATACLOUD_BASE_URL`).                        |
| `--endpoint`  | Full `pipelineRuns.upsert` URL (overrides `--base-url`).             |
| `--source`    | Default pipeline source name. Prompted for interactively if omitted. Overridden per-row by a CSV `source` column. |
| `--token-env` | Env var holding the API token. Defaults to `DATACLOUD_API_TOKEN`.  |
| `--dry-run`   | Print payloads instead of sending them.                             |
| `--limit N`   | Only process the first `N` data rows (across all files).            |
| `--sleep S`   | Seconds to wait between requests.                                   |
| `--timeout S` | Per-request timeout in seconds. Defaults to `30`.                   |
| `--fail-fast` | Stop after the first failed row.                                    |

The process exits with code `0` on full success, `1` if any row failed, and `2`
on configuration errors (e.g. missing token or base URL).

## Project layout

| File                              | Responsibility                                            |
| --------------------------------- | --------------------------------------------------------- |
| `main.py`                         | CLI entry point and argument parsing.                     |
| `config.py`                       | Settings, `.env` loading, and endpoint resolution.        |
| `uploader.py`                     | Reads the CSV and orchestrates per-row upserts.           |
| `pipeline_payload.py`             | Maps a CSV row to the API payload.                        |
| `datacloud_client.py`             | Minimal HTTP client for the DX API.                       |
| `data/`                           | Default drop-in directory for `*.csv` files.              |
| `sample_teamcity_pipeline_runs.csv` | Example input data.                                     |
