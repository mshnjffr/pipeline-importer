"""Small HTTP client for Datacloud API requests."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any

import aiohttp


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
        self._timeout = aiohttp.ClientTimeout(total=self.timeout_seconds)
        self._headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json",
        }

    async def upsert_pipeline_run(self, session: aiohttp.ClientSession, payload: dict[str, Any]) -> ApiResponse:
        try:
            async with session.post(
                self.endpoint,
                data=json.dumps(payload).encode("utf-8"),
                headers=self._headers,
                timeout=self._timeout,
            ) as response:
                body = await response.text()
                return ApiResponse(
                    ok=(response.status < 400 and self._body_is_ok(body)),
                    status_code=response.status,
                    body=body,
                )
        except asyncio.TimeoutError:
            return ApiResponse(False, None, "request timeout")
        except aiohttp.ClientError as exc:
            return ApiResponse(False, None, str(exc) or exc.__class__.__name__)
        except OSError as exc:
            return ApiResponse(False, None, str(exc) or exc.__class__.__name__)

    @staticmethod
    def _body_is_ok(body: str) -> bool:
        try:
            parsed = json.loads(body)
        except json.JSONDecodeError:
            return True

        return parsed.get("ok", True) is True
