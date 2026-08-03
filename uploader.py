"""CSV upload orchestration for pipelineRuns.upsert."""

from __future__ import annotations

import asyncio
import csv
import json
import time
from pathlib import Path
from threading import Event, Lock, Thread

import aiohttp

from config import ConfigError, Settings
from datacloud_client import DatacloudClient
from pipeline_payload import PipelineRunPayload


class PipelineRunUploader:
    PROGRESS_LOG_INTERVAL_SECONDS = 10.0
    SUCCESS_LOG_BATCH_SIZE = 100

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.mapper = PipelineRunPayload(settings.source)
        self._rows_seen = 0

        self._request_spacing_lock = asyncio.Lock()
        self._next_request_at = 0.0

        self._progress_lock = Lock()
        self._progress_started_at = 0.0
        self._progress_processed = 0
        self._progress_failures = 0
        self._successes_logged = 0
        self._progress_stop_event = Event()
        self._progress_thread: Thread | None = None

    def run(self) -> int:
        csv_files = self.settings.resolve_csv_files()
        missing = [path for path in csv_files if not path.exists()]
        if missing:
            raise ConfigError("CSV file(s) not found: " + ", ".join(str(path) for path in missing))

        started_at = time.perf_counter()
        client = self._client()
        print(f"Found {len(csv_files)} CSV file(s) to process.")
        if self.settings.skip:
            print(f"Skipping the first {self.settings.skip} row(s).")

        self._start_progress_reporter(started_at)
        try:
            processed, failures = asyncio.run(self._run_async(client, csv_files))
        finally:
            self._stop_progress_reporter()

        elapsed_seconds = time.perf_counter() - started_at
        rows_per_second = (processed / elapsed_seconds) if elapsed_seconds > 0 else 0.0
        rows_per_minute = rows_per_second * 60
        print(f"Processed {processed} row(s) across {len(csv_files)} file(s); failures: {failures}.")
        print(
            f"Elapsed: {elapsed_seconds:.2f}s | Throughput: "
            f"{rows_per_second:.2f} rows/s ({rows_per_minute:.2f} rows/min)"
        )
        return 1 if failures else 0

    async def _run_async(self, client: DatacloudClient | None, csv_files: list[Path]) -> tuple[int, int]:
        processed = 0
        failures = 0

        if self.settings.dry_run:
            for csv_file in csv_files:
                if self._budget_reached(processed):
                    break
                print(f"== {csv_file} ==")
                file_processed, file_failures, stop = self._process_file_dry_run(csv_file, processed)
                processed += file_processed
                failures += file_failures
                if stop:
                    break
            return processed, failures

        if not client:
            raise ConfigError("Missing API client")

        connector = aiohttp.TCPConnector(limit=max(self.settings.workers * 2, self.settings.workers))
        async with aiohttp.ClientSession(connector=connector) as session:
            for csv_file in csv_files:
                if self._budget_reached(processed):
                    break

                print(f"== {csv_file} ==")
                file_processed, file_failures, stop = await self._process_file_async(
                    client=client,
                    session=session,
                    csv_path=csv_file,
                    already_processed=processed,
                )
                processed += file_processed
                failures += file_failures
                if stop:
                    break

        return processed, failures

    async def _process_file_async(
        self,
        client: DatacloudClient,
        session: aiohttp.ClientSession,
        csv_path: Path,
        already_processed: int,
    ) -> tuple[int, int, bool]:
        """Return (rows processed, failures, whether to stop the whole run)."""

        processed = 0
        submitted = 0
        failures = 0
        stop = False
        workers = self.settings.workers
        pending: set[asyncio.Task[bool]] = set()

        with csv_path.open(newline="") as csv_file:
            reader = csv.DictReader(csv_file)

            for raw_row in reader:
                self._rows_seen += 1
                if self._rows_seen <= self.settings.skip:
                    continue

                if self._budget_reached(already_processed + submitted) or stop:
                    break

                submitted += 1
                pending.add(
                    asyncio.create_task(
                        self._process_row_with_optional_sleep_async(
                            client=client,
                            session=session,
                            raw_row=raw_row,
                            row_number=self._rows_seen,
                        )
                    )
                )

                if len(pending) >= workers:
                    completed, new_failures, should_stop = await self._drain_completed_async(
                        pending,
                        wait_for_one=True,
                    )
                    self._record_progress(completed, new_failures)
                    processed += completed
                    failures += new_failures
                    if should_stop:
                        stop = True

            if stop and self.settings.fail_fast:
                for task in pending:
                    task.cancel()

            completed, new_failures, _ = await self._drain_completed_async(pending, wait_for_one=False)
            self._record_progress(completed, new_failures)
            processed += completed
            failures += new_failures

        return processed, failures, stop

    def _process_file_dry_run(self, csv_path: Path, already_processed: int) -> tuple[int, int, bool]:
        processed = 0
        failures = 0

        with csv_path.open(newline="") as csv_file:
            reader = csv.DictReader(csv_file)

            for raw_row in reader:
                self._rows_seen += 1
                if self._rows_seen <= self.settings.skip:
                    continue

                if self._budget_reached(already_processed + processed):
                    return processed, failures, False

                processed += 1
                payload = self.mapper.from_csv_row(raw_row)
                print(json.dumps(payload, indent=2, sort_keys=True))
                self._record_progress(1, 0)

        return processed, failures, False

    async def _drain_completed_async(
        self,
        pending: set[asyncio.Task[bool]],
        *,
        wait_for_one: bool,
    ) -> tuple[int, int, bool]:
        if not pending:
            return 0, 0, False

        if wait_for_one:
            done, _ = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        else:
            done = set(pending)

        completed = 0
        failures = 0
        should_stop = False
        for task in done:
            pending.remove(task)
            if task.cancelled():
                continue
            completed += 1
            failed = await task
            failures += int(failed)
            if failed and self.settings.fail_fast:
                should_stop = True

        return completed, failures, should_stop

    async def _process_row_with_optional_sleep_async(
        self,
        client: DatacloudClient,
        session: aiohttp.ClientSession,
        raw_row: dict[str, str | None],
        row_number: int,
    ) -> bool:
        if self.settings.sleep_seconds:
            async with self._request_spacing_lock:
                now = time.monotonic()
                if now < self._next_request_at:
                    await asyncio.sleep(self._next_request_at - now)
                    now = time.monotonic()
                self._next_request_at = now + self.settings.sleep_seconds
        return await self._process_row_async(client, session, raw_row, row_number)

    def _start_progress_reporter(self, started_at: float) -> None:
        self._progress_started_at = started_at
        self._progress_processed = 0
        self._progress_failures = 0
        self._successes_logged = 0
        self._progress_stop_event.clear()
        self._progress_thread = Thread(target=self._progress_reporter_loop, daemon=True)
        self._progress_thread.start()

    def _stop_progress_reporter(self) -> None:
        self._progress_stop_event.set()
        if self._progress_thread:
            self._progress_thread.join()
            self._progress_thread = None

    def _record_progress(self, processed_delta: int, failures_delta: int) -> None:
        if processed_delta <= 0 and failures_delta <= 0:
            return

        with self._progress_lock:
            self._progress_processed += processed_delta
            self._progress_failures += failures_delta
            successes = self._progress_processed - self._progress_failures
            while successes - self._successes_logged >= self.SUCCESS_LOG_BATCH_SIZE:
                self._successes_logged += self.SUCCESS_LOG_BATCH_SIZE
                print(
                    f"[batch] ok {self.SUCCESS_LOG_BATCH_SIZE} rows "
                    f"(successes={self._successes_logged}, failures={self._progress_failures})"
                )

    def _progress_reporter_loop(self) -> None:
        while not self._progress_stop_event.wait(self.PROGRESS_LOG_INTERVAL_SECONDS):
            elapsed_seconds = time.perf_counter() - self._progress_started_at
            with self._progress_lock:
                processed = self._progress_processed
                failures = self._progress_failures
            rows_per_second = (processed / elapsed_seconds) if elapsed_seconds > 0 else 0.0
            rows_per_minute = rows_per_second * 60
            print(
                f"[progress] elapsed={elapsed_seconds:.0f}s processed={processed} "
                f"failures={failures} throughput={rows_per_second:.2f} rows/s "
                f"({rows_per_minute:.2f} rows/min)"
            )

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

    async def _process_row_async(
        self,
        client: DatacloudClient,
        session: aiohttp.ClientSession,
        raw_row: dict[str, str | None],
        row_number: int,
    ) -> bool:
        payload = self.mapper.from_csv_row(raw_row)
        reference_id = payload.get("reference_id") or "(no reference_id)"

        response = await client.upsert_pipeline_run(session, payload)
        if response.ok:
            return False

        status = response.status_code if response.status_code is not None else "network"
        print(f"[{row_number}] failed {reference_id} ({status}): {response.body}")
        return True
