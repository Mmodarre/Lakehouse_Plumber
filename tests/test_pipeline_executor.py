"""Worker-logger init unit test.

The worker-logger initializer contract: ``spawn`` workers
must attach exactly one ``NullHandler`` (and no stderr handler) so worker-side
log records never leak to the user's terminal. That initializer
(:func:`_init_worker_logger`) lives in ``_flowgroup_pool`` but is still
re-exported from ``executor`` (the import path tested here). The end-to-end
no-leak behaviour is covered by ``tests/test_generate_worker_log_leak.py``;
this is the focused unit.
"""


def test_init_worker_logger_attaches_only_null_handler():
    """After init, root logger has exactly one NullHandler and no stderr handler."""
    import logging

    from lhp.core.coordination.executor import _init_worker_logger

    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    try:
        _init_worker_logger(logging.WARNING)
        assert len(root.handlers) == 1, (
            f"expected 1 handler, got {len(root.handlers)}: "
            f"{[type(h).__name__ for h in root.handlers]}"
        )
        assert isinstance(root.handlers[0], logging.NullHandler)
        assert root.level == logging.WARNING
    finally:
        for h in list(root.handlers):
            root.removeHandler(h)
        for h in original_handlers:
            root.addHandler(h)
        root.setLevel(original_level)
