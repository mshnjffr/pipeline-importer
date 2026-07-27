#!/usr/bin/env python3
"""Upload CI/CD pipeline runs from a CSV file to Datacloud."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from config import (
    BASE_URL_ENV,
    CSV_DIR_ENV,
    DEFAULT_CSV_DIR,
    DEFAULT_ENV_FILE,
    TOKEN_ENV,
    ConfigError,
    EnvFile,
    Settings,
)
from uploader import PipelineRunUploader


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send pipeline run rows from CSV to Datacloud pipelineRuns.upsert."
    )
    parser.add_argument(
        "csv_paths",
        nargs="*",
        metavar="CSV",
        help=(
            "One or more CSV files or directories to upload. Directories are "
            f"searched for *.csv. Defaults to the drop-in directory ({DEFAULT_CSV_DIR})."
        ),
    )
    parser.add_argument(
        "--csv-dir",
        help=f"Drop-in directory of CSV files. Can also be set with {CSV_DIR_ENV}.",
    )
    parser.add_argument(
        "--env-file",
        default=str(DEFAULT_ENV_FILE),
        help=f"Path to a .env file containing {TOKEN_ENV}.",
    )
    parser.add_argument(
        "--base-url",
        help=f"Datacloud base URL. Can also be set with {BASE_URL_ENV}.",
    )
    parser.add_argument(
        "--endpoint",
        help="Full pipelineRuns.upsert URL. Overrides --base-url.",
    )
    parser.add_argument(
        "--source",
        help=(
            "Default pipeline source name (e.g. TeamCity, Jenkins, GitHub Actions). "
            "If omitted, you'll be prompted for it. A CSV row with its own 'source' "
            "column value overrides this default for that row."
        ),
    )
    parser.add_argument("--token-env", default=TOKEN_ENV, help=f"Token env var. Defaults to {TOKEN_ENV}.")
    parser.add_argument("--dry-run", action="store_true", help="Print payloads without sending them.")
    parser.add_argument("--limit", type=int, help="Only process the first N data rows.")
    parser.add_argument(
        "--skip",
        type=int,
        default=0,
        help=(
            "Skip the first N data rows (across all files) before processing. "
            "Use this to resume after a partial/interrupted run: the row numbers "
            "printed during a run are 1-indexed across all files, so --skip 9010 "
            "resumes right after row 9010."
        ),
    )
    parser.add_argument("--sleep", type=float, default=0.0, help="Seconds to wait between requests.")
    parser.add_argument("--timeout", type=int, default=30, help="Request timeout in seconds.")
    parser.add_argument("--fail-fast", action="store_true", help="Stop after the first failed row.")
    return parser.parse_args()


def resolve_source(cli_source: str | None) -> str:
    """Resolve the default pipeline source, prompting interactively if needed.

    Rows with their own 'source' column still override this per-row (see
    PipelineRunPayload), so this is only the fallback for rows that don't.
    """

    if cli_source:
        return cli_source

    if not sys.stdin.isatty():
        raise ConfigError(
            "No --source provided and input is not interactive. Pass --source "
            "(e.g. --source Jenkins) or add a 'source' column to your CSV."
        )

    try:
        source = input(
            "Which CI/CD system is this data from? (e.g. TeamCity, Jenkins, "
            "GitHub Actions): "
        ).strip()
    except EOFError:
        source = ""

    if not source:
        raise ConfigError(
            "A pipeline source is required. Pass --source or add a 'source' "
            "column to your CSV."
        )

    return source


def settings_from_args(args: argparse.Namespace, source: str) -> Settings:
    return Settings(
        csv_paths=tuple(Path(path) for path in args.csv_paths),
        csv_dir=Path(args.csv_dir) if args.csv_dir else None,
        env_file=Path(args.env_file),
        source=source,
        token_env=args.token_env,
        dry_run=args.dry_run,
        limit=args.limit,
        sleep_seconds=args.sleep,
        timeout_seconds=args.timeout,
        fail_fast=args.fail_fast,
        skip=args.skip,
        base_url=args.base_url,
        endpoint=args.endpoint,
    )


def main() -> int:
    args = parse_args()

    try:
        source = resolve_source(args.source)
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    settings = settings_from_args(args, source)
    EnvFile.load(settings.env_file)

    try:
        return PipelineRunUploader(settings).run()
    except ConfigError as exc:
        print(str(exc), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
