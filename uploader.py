"""CSV upload orchestration for pipelineRuns.upsert."""

from __future__ import annotations

import csv
import json
import time
from pathlib import Path

from config import ConfigError, Settings
from datacloud_client import ApiResponse, DatacloudClient
from pipeline_payload import PipelineRunPayload


class PipelineRunUploader:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.mapper = PipelineRunPayload(settings.source)

    def run(self) -> int:
        csv_files = self.settings.resolve_csv_files()
        missing = [path for path in csv_files if not path.exists()]
        if missing:
            raise ConfigError(
                "CSV file(s) not found: " + ", ".join(str(path) for path in missing)
            )

        client = self._client()
        print(f"Found {len(csv_files)} CSV file(s) to process.")

        processed = 0
        failures = 0
        for csv_file in csv_files:
            if self._budget_reached(processed):
                break

            print(f"== {csv_file} ==")
            file_processed, file_failures, stop = self._process_file(client, csv_file, processed)
            processed += file_processed
            failures += file_failures

            if stop:
                break

        print(f"Processed {processed} row(s) across {len(csv_files)} file(s); failures: {failures}.")
        return 1 if failures else 0

    def _process_file(
        self,
        client: DatacloudClient | None,
        csv_path: Path,
        already_processed: int,
    ) -> tuple[int, int, bool]:
        """Return (rows processed, failures, whether to stop the whole run)."""

        processed = 0
        failures = 0

        with csv_path.open(newline="") as csv_file:
            reader = csv.DictReader(csv_file)

            for raw_row in reader:
                if self._budget_reached(already_processed + processed):
                    return processed, failures, False

                processed += 1
                failed = self._process_row(client, raw_row, already_processed + processed)
                failures += int(failed)

                if failed and self.settings.fail_fast:
                    return processed, failures, True

                if self.settings.sleep_seconds and client:
                    time.sleep(self.settings.sleep_seconds)

        return processed, failures, False

    def _budget_reached(self, processed: int) -> bool:
        return self.settings.limit is not None and processed >= self.settings.limit

    def _client(self) -> DatacloudClient | None:
        if self.settings.dry_run:
            return None

        return DatacloudClient(
            self.settings.api_endpoint,
            self.settings.token,
            self.settings.timeout_seconds,
        )

    def _process_row(
        self,
        client: DatacloudClient | None,
        raw_row: dict[str, str | None],
        row_number: int,
    ) -> bool:
        payload = self.mapper.from_csv_row(raw_row)
        reference_id = payload.get("reference_id") or "(no reference_id)"

        if self.settings.dry_run:
            print(json.dumps(payload, indent=2, sort_keys=True))
            return False

        response = client.upsert_pipeline_run(payload) if client else ApiResponse(False, None, "missing client")
        if response.ok:
            print(f"[{row_number}] ok {reference_id} ({response.status_code})")
            return False

        status = response.status_code if response.status_code is not None else "network"
        print(f"[{row_number}] failed {reference_id} ({status}): {response.body}")
        return True
