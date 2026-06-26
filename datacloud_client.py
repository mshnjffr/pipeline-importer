"""Small HTTP client for Datacloud API requests."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


@dataclass(frozen=True)
class ApiResponse:
    ok: bool
    status_code: int | None
    body: str


class DatacloudClient:
    def __init__(self, endpoint: str, token: str, timeout_seconds: int) -> None:
        self.endpoint = endpoint
        self.token = token
        self.timeout_seconds = timeout_seconds

    def upsert_pipeline_run(self, payload: dict[str, Any]) -> ApiResponse:
        request = Request(
            self.endpoint,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )

        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read().decode("utf-8")
                return ApiResponse(self._body_is_ok(body), response.status, body)
        except HTTPError as exc:
            return ApiResponse(False, exc.code, exc.read().decode("utf-8"))
        except URLError as exc:
            return ApiResponse(False, None, str(exc.reason))

    @staticmethod
    def _body_is_ok(body: str) -> bool:
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            return True

        return parsed.get("ok", True) is True
