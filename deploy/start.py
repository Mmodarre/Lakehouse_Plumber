"""Databricks App entry point.

Reads ``$DATABRICKS_APP_PORT`` (injected by Databricks Apps runtime)
and starts the LHP FastAPI application via uvicorn.
"""

import logging
import os

logging.basicConfig(level=logging.INFO)


def main() -> None:
    import uvicorn

    port = int(os.environ.get("DATABRICKS_APP_PORT", "8000"))
    uvicorn.run(
        "lhp.api.app:create_app",
        host="0.0.0.0",
        port=port,
        factory=True,
        log_level="info",
    )


if __name__ == "__main__":
    main()
