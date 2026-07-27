"""Configuration helpers for the pipeline run CSV uploader."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


ENDPOINT_PATH = "/api/pipelineRuns.upsert"
TOKEN_ENV = "DATACLOUD_API_TOKEN"
BASE_URL_ENV = "DATACLOUD_BASE_URL"
CSV_DIR_ENV = "DATACLOUD_CSV_DIR"
DEFAULT_ENV_FILE = Path(".env")
DEFAULT_CSV_DIR = Path(__file__).with_name("data")
CSV_GLOB = "*.csv"


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class Settings:
    csv_paths: tuple[Path, ...]
    env_file: Path
    source: str
    token_env: str
    dry_run: bool
    limit: int | None
    sleep_seconds: float
    timeout_seconds: int
    fail_fast: bool
    skip: int
    base_url: str | None = None
    endpoint: str | None = None
    csv_dir: Path | None = None

    def resolve_csv_files(self) -> list[Path]:
        """Expand the configured paths into an ordered, de-duplicated CSV list.

        Each entry may be a file or a directory; directories are globbed for
        ``*.csv`` and sorted so processing order is deterministic (first-first).
        When no paths are given, fall back to the drop-in directory.
        """

        inputs = list(self.csv_paths) or [self._csv_dir()]

        files: list[Path] = []
        for path in inputs:
            if path.is_dir():
                files.extend(sorted(path.glob(CSV_GLOB)))
            else:
                files.append(path)

        seen: set[Path] = set()
        ordered: list[Path] = []
        for file in files:
            resolved = file.resolve()
            if resolved not in seen:
                seen.add(resolved)
                ordered.append(file)

        if not ordered:
            raise ConfigError(
                f"No CSV files found. Pass a CSV/directory or drop *.csv into {self._csv_dir()}."
            )

        return ordered

    def _csv_dir(self) -> Path:
        configured = self.csv_dir or os.environ.get(CSV_DIR_ENV)
        return Path(configured) if configured else DEFAULT_CSV_DIR

    @property
    def token(self) -> str:
        token = os.environ.get(self.token_env)
        if token or self.dry_run:
            return token or ""

        raise ConfigError(f"Missing API token. Add {self.token_env}=... to {self.env_file}.")

    @property
    def api_endpoint(self) -> str:
        if self.dry_run:
            return ""

        if self.endpoint:
            return self.endpoint

        base_url = self.base_url or os.environ.get(BASE_URL_ENV)
        if not base_url:
            raise ConfigError(f"Missing base URL. Set {BASE_URL_ENV} in .env or pass --base-url.")

        return endpoint_from_base_url(base_url)


class EnvFile:
    @staticmethod
    def load(path: Path) -> None:
        if not path.exists():
            return

        for raw_line in path.read_text().splitlines():
            key_value = EnvFile._parse_line(raw_line)
            if not key_value:
                continue

            key, value = key_value
            os.environ.setdefault(key, value)

    @staticmethod
    def _parse_line(raw_line: str) -> tuple[str, str] | None:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            return None

        if line.startswith("export "):
            line = line[len("export ") :].strip()

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'\"")
        return (key, value) if key else None


def endpoint_from_base_url(base_url: str) -> str:
    base_url = base_url.rstrip("/")
    if base_url.endswith(ENDPOINT_PATH):
        return base_url
    if base_url.endswith("/api"):
        return f"{base_url}/pipelineRuns.upsert"
    return f"{base_url}{ENDPOINT_PATH}"
