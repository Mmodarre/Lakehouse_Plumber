"""Read-only Bash command classifier for the Claude SDK approval policy.

Pure, dependency-free classification: :func:`is_read_only_command` decides
whether a shell command is provably read-only and may run without an
approval prompt. FAIL-CLOSED by design — any parse doubt, any operator
beyond the plain connectors, any command or flag outside the explicit
allowlist returns ``False``. A ``False`` merely means the user gets the
normal approval prompt, so over-rejection is harmless; over-acceptance
would be a security hole.

:stability: internal
"""

from __future__ import annotations

import shlex

#: Characters that force rejection before any parsing: command substitution
#: (``$(...)`` / backticks), variable expansion, and multi-line trickery.
_HARD_REJECT_CHARS = ("`", "$", "\n", "\r")

#: The shlex punctuation set (``();<>|&``) — any token made purely of these
#: is a shell operator, never an argument.
_PUNCTUATION = frozenset("();<>|&")

#: Operators allowed to join read-only segments. Everything else —
#: ``&``, any redirection, subshells — rejects the whole command.
_CONNECTORS = frozenset({"|", "&&", "||", ";"})

#: Commands safe with any flags (they cannot write through flags alone).
_PLAIN_SAFE = frozenset(
    {
        "ls", "cat", "head", "tail", "wc", "file", "stat", "du", "df",
        "pwd", "tree", "basename", "dirname", "realpath", "uniq", "cut",
        "diff", "which", "whoami", "date", "echo", "printf", "md5",
        "shasum", "column", "nl", "grep", "rg",
    }
)  # fmt: skip

#: ``find`` actions that execute or write; ``-fprint*`` is prefix-matched.
_FIND_REJECT = frozenset({"-delete", "-exec", "-execdir", "-ok", "-okdir", "-fls"})

#: Read-only git subcommands. ``branch``/``remote``/``tag`` get extra
#: per-subcommand flag rules below (they can mutate refs).
_GIT_SUBCOMMANDS = frozenset(
    {
        "status", "log", "diff", "show", "shortlog", "describe",
        "rev-parse", "blame", "ls-files", "branch", "remote", "tag",
    }
)  # fmt: skip

#: ``git branch`` flags that create/move/delete refs.
_GIT_BRANCH_REJECT = frozenset(
    {"-d", "-D", "-m", "-M", "-c", "-C", "--delete", "--move", "--copy"}
)

#: ``git remote`` / ``git tag`` may ONLY list: bare or with these flags.
_GIT_LIST_FLAGS = frozenset({"-v", "-l", "--list"})


def _git_segment_is_read_only(args: list[str]) -> bool:
    """Read-only check for a ``git ...`` argv (``args`` excludes ``git``)."""
    for arg in args:
        # `-o`/`--output*` redirect git output to a file — anywhere in argv.
        if arg == "-o" or arg.startswith("--output"):
            return False
    index = 0
    while index < len(args) and args[index] in ("--no-pager", "-P"):
        index += 1
    if index >= len(args):
        return False  # bare `git` (or only global flags): nothing to vet
    subcommand = args[index]
    if subcommand not in _GIT_SUBCOMMANDS:
        return False
    rest = args[index + 1 :]
    if subcommand == "branch":
        return not any(
            arg in _GIT_BRANCH_REJECT or arg.startswith("--set-upstream")
            for arg in rest
        )
    if subcommand in ("remote", "tag"):
        # Only listing forms: `git remote add ...` / `git tag v1` must prompt.
        return all(arg in _GIT_LIST_FLAGS for arg in rest)
    return True


def _segment_is_read_only(argv: list[str]) -> bool:
    """Whether one pipeline segment (a simple command) is provably read-only."""
    name = argv[0].rsplit("/", 1)[-1]
    if name in _PLAIN_SAFE:
        return True
    if name == "sort":
        # `-o<file>`/`--output=<file>` write; prefix-match catches both forms.
        return not any(arg.startswith(("-o", "--output")) for arg in argv[1:])
    if name == "find":
        return not any(
            arg in _FIND_REJECT or arg.startswith("-fprint") for arg in argv[1:]
        )
    if name == "git":
        return _git_segment_is_read_only(argv[1:])
    return False


def is_read_only_command(command: str) -> bool:
    """``True`` only when ``command`` is provably read-only (fail-closed).

    Rejects outright: empty commands; any ``$``/backtick/newline (expansion
    and substitution); unparseable quoting; any redirection or operator
    other than ``|``, ``&&``, ``||``, ``;``. Each connector-separated
    segment must then start with an allowlisted command and pass that
    command's flag rules.
    """
    if not command or not command.strip():
        return False
    if any(char in command for char in _HARD_REJECT_CHARS):
        return False
    lexer = shlex.shlex(command, posix=True, punctuation_chars=True)
    lexer.whitespace_split = True
    try:
        tokens = list(lexer)
    except ValueError:
        return False

    segments: list[list[str]] = [[]]
    for token in tokens:
        if token and set(token) <= _PUNCTUATION:
            if token not in _CONNECTORS:
                return False  # redirection, `&`, subshell, `>>`, `<<`, ...
            segments.append([])
        else:
            segments[-1].append(token)
    return all(segment and _segment_is_read_only(segment) for segment in segments)
