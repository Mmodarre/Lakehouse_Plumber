"""Read release metadata from the fixed PyPI HTTPS endpoint without redirects."""

import http.client
import json
import re
from contextlib import closing


def fetch_metadata(version: str) -> dict | None:
    """Return PyPI metadata, or None only for a direct HTTP 404 response.

    The host and TLS transport are fixed. Reject every redirect so a response
    cannot change the destination or downgrade transport during publication.
    Network, TLS, HTTP and JSON errors propagate to stop the release.
    """
    if not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise ValueError("Release version must be a stable three-part version")
    with closing(http.client.HTTPSConnection("pypi.org", timeout=30)) as connection:
        connection.request("GET", f"/pypi/lakehouse-plumber/{version}/json")
        with connection.getresponse() as response:
            if response.status == 404:
                return None
            if response.status != 200:
                raise RuntimeError(
                    f"PyPI metadata request failed with HTTP {response.status}; "
                    "redirects are not permitted"
                )
            metadata = json.load(response)
    if not isinstance(metadata, dict):
        raise TypeError("PyPI metadata must be a JSON object")
    return metadata
