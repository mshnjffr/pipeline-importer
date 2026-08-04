"""CSV row to pipelineRuns.upsert payload mapping."""

from __future__ import annotations

from datetime import datetime
from typing import Any


class PipelineRunPayload:
    FIELDS = (
        "reference_id",
        "pipeline_name",
        "status",
        "started_at",
        "finished_at",
        "repository",
        "commit_sha",
        "head_branch",
        "pr_number",
        "email",
        "github_username",
        "gitlab_username",
        "source_url",
    )

    def __init__(self, default_source: str) -> None:
        self.default_source = default_source

    def from_csv_row(self, raw_row: dict[str, str | None]) -> dict[str, Any]:
        row = self._clean_row(raw_row)

        payload: dict[str, Any] = {
            "pipeline_source": row.get("source") or self.default_source
        }
        for field in self.FIELDS:
            value = row.get(field)
            if value:
                if field in {"started_at", "finished_at"}:
                    payload[field] = self._normalize_timestamp(value)
                else:
                    payload[field] = value

        return payload

    @staticmethod
    def _clean_row(row: dict[str, str | None]) -> dict[str, str]:
        return {
            key.strip(): (value or "").strip()
            for key, value in row.items()
            if key is not None
        }

    @staticmethod
    def _normalize_timestamp(value: str) -> str:
        candidate = value.strip()
        if not candidate:
            return candidate

        parse_value = f"{candidate[:-1]}+00:00" if candidate.endswith("Z") else candidate
        try:
            parsed = datetime.fromisoformat(parse_value)
        except ValueError:
            return candidate

        if parsed.tzinfo is None:
            return f"{parsed.isoformat(timespec='seconds')}Z"
        return parsed.isoformat(timespec="seconds").replace("+00:00", "Z")
