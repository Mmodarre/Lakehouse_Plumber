"""Release metadata transport must stay on PyPI HTTPS and fail closed."""

import importlib.util
import io
import os
import ssl
import subprocess
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "_release_pypi.py"
spec = importlib.util.spec_from_file_location("release_pypi", SCRIPT)
assert spec and spec.loader
pypi = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pypi)


@pytest.fixture
def transport(monkeypatch):
    response = io.BytesIO(b'{"info": {"version": "1.2.3"}}')
    response.status = 200
    connection = Mock()
    connection.getresponse.return_value = response
    factory = Mock(return_value=connection)
    monkeypatch.setattr("http.client.HTTPSConnection", factory)
    return factory, connection, response


def test_fixed_tls_host_and_path_with_timeout(transport):
    factory, connection, response = transport
    assert pypi.fetch_metadata("1.2.3") == {"info": {"version": "1.2.3"}}
    factory.assert_called_once_with("pypi.org", timeout=30)
    connection.request.assert_called_once_with(
        "GET", "/pypi/lakehouse-plumber/1.2.3/json"
    )
    connection.close.assert_called_once()
    assert response.closed


@pytest.mark.parametrize(
    "version",
    ["file:///etc/passwd", "https://example.com", "../1.2.3", "1.2.3?x=1", "1.2.3\n"],
)
def test_version_cannot_supply_a_url_or_change_the_path(transport, version):
    factory, _, _ = transport
    with pytest.raises(ValueError, match="three-part version"):
        pypi.fetch_metadata(version)
    factory.assert_not_called()


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
@pytest.mark.parametrize(
    "location",
    [
        "file:///etc/passwd",
        "http://pypi.org/metadata",
        "https://example.com/metadata",
        "custom:metadata",
        "/pypi/lakehouse-plumber/1.2.3/json",
    ],
)
def test_redirects_are_rejected_without_a_second_request(transport, status, location):
    factory, connection, response = transport
    response.status = status
    response.headers = {"Location": location}
    with pytest.raises(RuntimeError, match="redirects are not permitted"):
        pypi.fetch_metadata("1.2.3")
    assert factory.call_count == 1
    assert connection.request.call_count == 1
    connection.close.assert_called_once()
    assert response.closed


def test_only_direct_404_means_missing(transport):
    _, connection, response = transport
    response.status = 404
    assert pypi.fetch_metadata("1.2.3") is None
    connection.close.assert_called_once()
    assert response.closed


@pytest.mark.parametrize("status", [204, 304, 400, 401, 403, 429, 500, 503])
def test_other_http_statuses_fail_closed(transport, status):
    _, connection, response = transport
    response.status = status
    with pytest.raises(RuntimeError, match=f"HTTP {status}"):
        pypi.fetch_metadata("1.2.3")
    connection.close.assert_called_once()
    assert response.closed


@pytest.mark.parametrize("body", [b"not JSON", b"null", b"[]"])
def test_invalid_metadata_fails_closed_and_closes_transport(transport, body):
    _, connection, _ = transport
    response = io.BytesIO(body)
    response.status = 200
    connection.getresponse.return_value = response
    with pytest.raises((ValueError, TypeError)):
        pypi.fetch_metadata("1.2.3")
    connection.close.assert_called_once()
    assert response.closed


@pytest.mark.parametrize("name", ["release_metadata.py", "publish_release_assets.py"])
def test_release_scripts_run_directly_without_pythonpath(tmp_path, name):
    (tmp_path / "pyproject.toml").write_text(
        '[project]\nname="lakehouse-plumber"\nversion="invalid"\n'
    )
    result = subprocess.run(
        [sys.executable, str(SCRIPT.with_name(name)), str(tmp_path)],
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": "", "VERSION": "invalid"},
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode != 0
    assert "Release version must be a stable three-part version" in result.stderr


@pytest.mark.parametrize("operation", ["request", "getresponse"])
@pytest.mark.parametrize(
    "error",
    [TimeoutError("timeout"), ssl.SSLCertVerificationError("invalid certificate")],
)
def test_network_errors_propagate_and_close_transport(transport, operation, error):
    _, connection, _ = transport
    getattr(connection, operation).side_effect = error
    with pytest.raises(type(error)):
        pypi.fetch_metadata("1.2.3")
    connection.close.assert_called_once()
