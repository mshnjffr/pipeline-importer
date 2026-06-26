"""CSV row to pipelineRuns.upsert payload mapping."""

from __future__ import annotations

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

    def __init__(self, source: str) -> None:
        self.source = source

    def from_csv_row(self, raw_row: dict[str, str | None]) -> dict[str, Any]:
        row = self._clean_row(raw_row)

        payload: dict[str, Any] = {"pipeline_source": self.source}
        for field in self.FIELDS:
            value = row.get(field)
            if value:
                payload[field] = value

        return payload

    @staticmethod
    def _clean_row(row: dict[str, str | None]) -> dict[str, str]:
        return {
            key.strip(): (value or "").strip()
            for key, value in row.items()
            if key is not None
        }
