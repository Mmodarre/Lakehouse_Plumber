"""Tests for the _proxy_request helper in the AI router.

Verifies that transport-level errors (timeout, connect, HTTP) are
translated into appropriate HTTP status codes rather than surfacing
as unhandled 500 Internal Server Errors.
"""

import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock, patch

from lhp.api.routers.ai import _proxy_request


@pytest.fixture
def mock_response():
    """Build an httpx.Response with the given status code."""

    def _make(status_code: int = 200, text: str = "ok"):
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = status_code
        resp.text = text
        return resp

    return _make


@pytest.mark.asyncio
async def test_proxy_request_success(mock_response):
    """Successful request returns the response object."""
    resp = mock_response(200)
    mock_client = AsyncMock()
    mock_client.request = AsyncMock(return_value=resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("lhp.api.routers.ai.httpx.AsyncClient", return_value=mock_client):
        result = await _proxy_request(
            "GET",
            "http://localhost:3000/session",
            headers={"Authorization": "Bearer test"},
        )
    assert result.status_code == 200


@pytest.mark.asyncio
async def test_proxy_request_timeout_returns_504(mock_response):
    """httpx.TimeoutException is translated to 504 Gateway Timeout."""
    mock_client = AsyncMock()
    mock_client.request = AsyncMock(side_effect=httpx.TimeoutException("timed out"))
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("lhp.api.routers.ai.httpx.AsyncClient", return_value=mock_client):
        with pytest.raises(Exception) as exc_info:
            await _proxy_request(
                "POST",
                "http://localhost:3000/session",
                headers={},
                timeout=5.0,
            )
    assert exc_info.value.status_code == 504
    assert "timed out" in str(exc_info.value.detail).lower()


@pytest.mark.asyncio
async def test_proxy_request_connect_error_returns_502(mock_response):
    """httpx.ConnectError is translated to 502 Bad Gateway."""
    mock_client = AsyncMock()
    mock_client.request = AsyncMock(
        side_effect=httpx.ConnectError("Connection refused")
    )
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("lhp.api.routers.ai.httpx.AsyncClient", return_value=mock_client):
        with pytest.raises(Exception) as exc_info:
            await _proxy_request(
                "GET",
                "http://localhost:3000/session",
                headers={},
            )
    assert exc_info.value.status_code == 502
    assert "cannot reach" in str(exc_info.value.detail).lower()


@pytest.mark.asyncio
async def test_proxy_request_http_error_returns_502(mock_response):
    """Generic httpx.HTTPError is translated to 502 Bad Gateway."""
    mock_client = AsyncMock()
    mock_client.request = AsyncMock(
        side_effect=httpx.HTTPError("something broke")
    )
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("lhp.api.routers.ai.httpx.AsyncClient", return_value=mock_client):
        with pytest.raises(Exception) as exc_info:
            await _proxy_request(
                "GET",
                "http://localhost:3000/session",
                headers={},
            )
    assert exc_info.value.status_code == 502
    assert "communication error" in str(exc_info.value.detail).lower()


@pytest.mark.asyncio
async def test_proxy_request_unexpected_status_raises(mock_response):
    """Response with unexpected status code raises HTTPException."""
    resp = mock_response(500, "Internal Server Error")
    mock_client = AsyncMock()
    mock_client.request = AsyncMock(return_value=resp)
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=False)

    with patch("lhp.api.routers.ai.httpx.AsyncClient", return_value=mock_client):
        with pytest.raises(Exception) as exc_info:
            await _proxy_request(
                "GET",
                "http://localhost:3000/session",
                headers={},
                expected_statuses=(200,),
            )
    assert exc_info.value.status_code == 500
