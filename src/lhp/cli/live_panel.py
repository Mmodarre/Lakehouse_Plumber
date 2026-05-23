"""Shared Rich Live-panel primitives for the generate and validate CLIs."""

import functools
import logging
import threading
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from time import perf_counter
from typing import Dict, Iterator, List, Optional, Tuple

from rich.console import Console as _RichConsole
from rich.console import Group, RenderableType
from rich.logging import RichHandler
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)
from rich.spinner import Spinner
from rich.table import Table
from rich.text import Text

logger = logging.getLogger(__name__)

# 250ms matches the visual refresh budget at ``refresh_per_second=10``;
# shorter phase markers would only flicker.
_PHASE_MARKER_MIN_DURATION_S = 0.25

_BRAND_ORANGE = "#E07A2C"

_LHP_WORDMARK = """\
██╗     ██╗  ██╗██████╗
██║     ██║  ██║██╔══██╗
██║     ███████║██████╔╝
██║     ██╔══██║██╔═══╝
███████╗██║  ██║██║
╚══════╝╚═╝  ╚═╝╚═╝     """

# Widest wordmark row + 1-char margin each side; fixes the right column
# width so the wordmark cannot wrap.
_WORDMARK_COL_WIDTH = 26

# Wordmark column + 60-char minimum left column + ~4 chars of panel
# border/grid padding. Below this, render the single-column fallback.
_WORDMARK_MIN_PANEL_WIDTH = _WORDMARK_COL_WIDTH + 60 + 4


@functools.cache
def _get_lhp_version() -> str:
    """Return the installed ``lakehouse-plumber`` version, or ``'?'`` on miss.

    Lazy import keeps this module free of a CLI-layer import cycle. Cached
    because ``render_live_frame`` calls this on every Rich Live refresh.
    """
    try:
        from importlib.metadata import version as _pkg_version

        return _pkg_version("lakehouse-plumber")
    except Exception:
        return "?"


@dataclass
class PipelineRecord:
    """In-memory tracking for one pipeline during a generate/validate run.

    Generate populates ``files`` / ``duration_s``; validate populates
    ``errors_count`` / ``warnings_count``. ``kind`` discriminates the
    two for ``ActivityTail.render``.
    """

    name: str
    success: Optional[bool] = None
    files: int = 0
    duration_s: float = 0.0
    error_code: Optional[str] = None
    errors_count: int = 0
    warnings_count: int = 0
    kind: str = "generate"


@dataclass
class PhaseTracker:
    """Tracks discrete phases of work and renders them in the live panel."""

    completed: List[Tuple[str, float, bool]] = field(default_factory=list)
    active: Optional[str] = None

    _start_times: Dict[str, float] = field(default_factory=dict)
    _active_spinner: Optional[Spinner] = field(default=None)
    _active_spinner_name: Optional[str] = field(default=None)
    _lock: threading.Lock = field(
        default_factory=threading.Lock, init=False, repr=False
    )

    def start(self, name: str) -> None:
        with self._lock:
            self._start_times[name] = perf_counter()
            self.active = name

    def complete(
        self,
        name: str,
        *,
        success: bool = True,
        suppress_if_fast: bool = False,
        label: Optional[str] = None,
    ) -> None:
        """Mark a phase as complete.

        ``suppress_if_fast`` drops phases shorter than 250ms.
        """
        with self._lock:
            start = self._start_times.pop(name, None)
            if start is None:
                logger.debug(
                    f"PhaseTracker.complete called for unknown phase {name!r}; ignoring"
                )
                return
            duration = perf_counter() - start
            if suppress_if_fast and duration < _PHASE_MARKER_MIN_DURATION_S:
                if self.active == name:
                    self.active = None
                return
            render_label = label if label is not None else name
            self.completed.append((render_label, duration, success))
            if self.active == name:
                self.active = None

    def render(self) -> RenderableType:
        with self._lock:
            elements: List[RenderableType] = []
            for label, duration_s, success in self.completed:
                marker = "✓" if success else "✗"
                marker_style = "bold green" if success else "bold red"
                elements.append(
                    Text.assemble(
                        ("  ", "default"),
                        (label.ljust(36), "dim"),
                        (marker, marker_style),
                        (f"   {duration_s:.1f}s", "dim"),
                    )
                )
            if self.active is None:
                self._active_spinner = None
                self._active_spinner_name = None
            else:
                if self._active_spinner_name != self.active:
                    self._active_spinner = Spinner(
                        "dots",
                        text=Text(self.active, style="bold cyan"),
                    )
                    self._active_spinner_name = self.active
                elements.append(self._active_spinner)
            return Group(*elements)


@dataclass
class ActivityTail:
    """Bounded ring of recently completed pipelines, rendered as a dim tail."""

    max_entries: int = 5
    _entries: "deque[PipelineRecord]" = field(init=False)

    def __post_init__(self) -> None:
        self._entries = deque(maxlen=self.max_entries)

    def append(self, record: PipelineRecord) -> None:
        self._entries.append(record)

    def has_entries(self) -> bool:
        return bool(self._entries)

    def render(self) -> RenderableType:
        # list() over a deque is atomic in CPython; safe while the main
        # thread may append concurrently.
        snapshot = list(self._entries)
        if not snapshot:
            return Text("")
        elements: List[RenderableType] = []
        for record in snapshot:
            if record.success:
                if record.kind == "validate":
                    elements.append(
                        Text.assemble(
                            ("  ", "default"),
                            ("✓", "bold green"),
                            (f" {record.name.ljust(24)}", "dim"),
                            (f" {record.errors_count} errors", "dim"),
                            (f"   {record.warnings_count} warnings", "dim"),
                        )
                    )
                else:
                    elements.append(
                        Text.assemble(
                            ("  ", "default"),
                            ("✓", "bold green"),
                            (f" {record.name.ljust(24)}", "dim"),
                            (f" {record.files} files", "dim"),
                            (f"   {record.duration_s:.1f}s", "dim"),
                        )
                    )
            else:
                code = record.error_code or "—"
                elements.append(
                    Text.assemble(
                        ("  ", "default"),
                        ("✗", "bold red"),
                        (f" {record.name.ljust(24)}", "dim"),
                        (f" {code}", "red"),
                    )
                )
        return Group(*elements)


class OverallProgress:
    """Single Rich Progress bar driven by the outer Live refresh.

    The Progress is used purely as a renderable embedded in the outer Panel;
    its internal ``rich.live.Live`` is never started. Calling
    ``Progress.start()`` while a parent ``Live`` is active stacks Lives on
    ``console._live_stack``: the topmost Live composes every stacked Live's
    ``get_renderable()`` into a Group, double-painting the bar and (with the
    internal Live's ``transient=False`` default) leaving an orphan after
    teardown. ``auto_refresh=False`` is belt-and-braces.
    """

    def __init__(
        self,
        label: str,
        total: int,
        *,
        console: Optional[_RichConsole] = None,
    ) -> None:
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console,
            auto_refresh=False,
        )
        # ``start=False`` keeps TimeElapsedColumn at ``-:--:--`` until the
        # caller signals work has begun.
        self._task_id = self._progress.add_task(label, total=total, start=False)
        self._started = False

    def start(self) -> None:
        # Deliberately NOT calling ``self._progress.start()`` — see class
        # docstring on the duplicate-bar hazard.
        self._progress.start_task(self._task_id)
        self._started = True

    def stop(self) -> None:
        if not self._started:
            return
        self._progress.stop_task(self._task_id)

    def advance(self, n: int = 1) -> None:
        self._progress.update(self._task_id, advance=n)

    def set_total(self, total: int) -> None:
        self._progress.update(self._task_id, total=total)

    def render(self) -> RenderableType:
        return self._progress


@dataclass(frozen=True)
class HeaderContext:
    command_name: str  # "generate" or "validate"
    env: str
    total_pipelines: int


def render_live_frame(
    phase_tracker: PhaseTracker,
    overall_progress: OverallProgress,
    activity_tail: ActivityTail,
    failure_lines: List[Text],
    *,
    header_context: HeaderContext,
    elapsed_text: str,
    show_progress: bool,
    failed_count: int = 0,
    console_width: int = 120,
) -> Panel:
    """Compose the persistent Live frame Panel.

    Pure function — no mutation, no I/O. The title is a ``rich.text.Text``
    instance, so callers asserting on title content must use
    ``panel.title.plain``.
    """
    title = Text(
        f"LHP {header_context.command_name} — env={header_context.env} "
        f"— pipelines={header_context.total_pipelines} — elapsed {elapsed_text}"
    )
    if failed_count > 0:
        title.append(Text(f" — {failed_count} failed", style="red bold"))

    has_recent = activity_tail.has_entries()
    body_elements: List[RenderableType] = [phase_tracker.render()]
    if show_progress:
        body_elements.append(overall_progress.render())
    body_elements.extend(
        [
            Text(""),
            Text("Recent:", style="dim") if has_recent else Text(""),
            activity_tail.render(),
            *failure_lines,
        ]
    )
    left_body = Group(*body_elements)

    body: RenderableType
    if console_width >= _WORDMARK_MIN_PANEL_WIDTH:
        wordmark = Text(_LHP_WORDMARK, style=f"bold {_BRAND_ORANGE}")
        version_tag = Text(
            f"v{_get_lhp_version()}",
            style=f"dim {_BRAND_ORANGE}",
            justify="right",
        )
        right_col = Group(wordmark, Text(""), version_tag)
        grid = Table.grid(expand=True, padding=(0, 1))
        grid.add_column(ratio=1)
        grid.add_column(width=_WORDMARK_COL_WIDTH)
        grid.add_row(left_body, right_col)
        body = grid
    else:
        body = left_body

    return Panel(body, title=title, title_align="left", border_style="dim")


class _LiveUpdateCoalescer:
    """Drops live.update calls within ``min_interval_s`` of the last.

    Throttles the burst of update calls during executor-submit so that large
    N does not block the main thread re-rendering the Panel before any worker
    finishes. The 100ms default aligns with ``Live(refresh_per_second=10)``.
    """

    def __init__(self, live, render_fn, min_interval_s: float = 0.1) -> None:
        self._live = live
        self._render_fn = render_fn
        self._min_interval_s = min_interval_s
        self._last_update = 0.0

    def update(self, *, force: bool = False) -> None:
        now = perf_counter()
        if force or (now - self._last_update) >= self._min_interval_s:
            self._live.update(self._render_fn())
            self._last_update = now


@contextmanager
def rich_handler_attached(err_console: _RichConsole) -> Iterator[None]:
    """Swap stream log handlers for a RichHandler during a Live frame.

    The teardown order matters: ``cli_error_boundary`` must print to a stderr
    with no Rich handler still attached, otherwise Rich's redirect_stderr
    machinery from a closed Live frame causes line-gluing on the final panel
    border. The context manager guarantees: Live exits -> handler detaches
    -> raise propagates -> cli_error_boundary prints through clean stderr.
    """
    root_logger = logging.getLogger()
    rich_handler = RichHandler(
        console=err_console,
        show_time=False,
        show_level=True,
        show_path=False,
        markup=False,
    )
    rich_handler.setLevel(logging.WARNING)
    removed_handlers: List[logging.Handler] = []
    for h in list(root_logger.handlers):
        if isinstance(h, logging.StreamHandler) and not isinstance(
            h, logging.FileHandler
        ):
            root_logger.removeHandler(h)
            removed_handlers.append(h)
    root_logger.addHandler(rich_handler)
    try:
        yield
    finally:
        root_logger.removeHandler(rich_handler)
        for h in removed_handlers:
            root_logger.addHandler(h)


def make_summary_table(title: str) -> Table:
    """Build the shared skeleton for generate/validate post-run summary tables.

    Returns a Rich ``Table`` with status-icon and Pipeline columns; callers
    append their own data columns.
    """
    table = Table(
        title=title,
        title_style="bold",
        show_header=True,
        header_style="bold dim",
        border_style="dim",
    )
    table.add_column("", width=2)
    table.add_column("Pipeline")
    return table


def warning_suffix_parts(warning_count: int) -> List[Tuple[str, str]]:
    """Return Rich ``Text.assemble`` parts for the warning-count suffix.

    Empty list when ``warning_count`` is zero so callers can splat
    unconditionally.
    """
    if warning_count == 0:
        return []
    noun = "warning" if warning_count == 1 else "warnings"
    return [
        ("; ", "default"),
        (f"{warning_count} {noun}", "bold yellow"),
    ]
