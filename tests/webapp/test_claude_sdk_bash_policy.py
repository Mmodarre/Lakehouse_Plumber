"""Table-driven tests for the read-only Bash classifier (fail-closed).

This is the security core of the assistant's prompt-reduction feature:
every SAFE entry runs without an approval prompt, so each one must be
provably read-only; every doubt case belongs in UNSAFE (a ``False`` merely
prompts the user).
"""

from __future__ import annotations

import pytest

from lhp.webapp.services.claude_sdk_bash_policy import is_read_only_command

pytestmark = pytest.mark.webapp

SAFE = [
    # plain-safe commands, bare and with flags
    "git status",
    "git log --oneline -5",
    'rg -n "foo" src',
    "find . -name '*.py'",
    "grep a | head -3",
    "ls && pwd",
    "git --no-pager diff",
    "cat file.txt",
    "du -sh .",
    "git branch -v",
    "git remote -v",
    "git tag --list",
    "ls",
    "ls -la /tmp",
    "pwd",
    "wc -l file.py",
    "head -n 20 f.txt",
    "tail -n 5 log.txt",
    "stat f",
    "df -h",
    "tree src",
    "basename /a/b",
    "dirname /a/b",
    "realpath .",
    "uniq f",
    "cut -d, -f1 f.csv",
    "diff a b",
    "which python",
    "whoami",
    "date",
    "echo hello world",
    "printf hi",
    "shasum -a 256 f",
    "md5 f",
    "column -t f",
    "nl f",
    "file f",
    # sort without output flags
    "sort f",
    "sort -r f | uniq",
    # find without write/exec actions
    "find src -type f -name '*.yaml' -print",
    # connectors between read-only segments
    "git status || git log",
    "cat a; cat b",
    "ls | grep foo | wc -l",
    # git read-only subcommands and pager-skip flags
    "git -P log",
    "git --no-pager -P status",
    "git show HEAD~1",
    "git shortlog -sn",
    "git describe --tags",
    "git rev-parse HEAD",
    "git blame f.py",
    "git ls-files",
    "git branch",
    "git branch -a",
    "git branch --list",
    "git remote",
    "git remote -v -l",
    "git tag -l",
    "git tag",
    # absolute path resolves by basename
    "/bin/ls -la",
    # quoted operator characters stay arguments (or fail closed, never open)
    'grep "a|b" f.txt',
]

UNSAFE = [
    # not allowlisted at all
    "rm -rf /",
    "grep a | tee /etc/passwd",
    "bash -c 'rm x'",
    "sh -c ls",
    'python -c "print(1)"',
    "sed -i s/a/b/ f",
    "curl http://x",
    "npm test",
    "gitfoo status",
    "xargs rm",
    "awk '{print}' f",
    "env ls",
    "wget http://x",
    "zsh -c ls",
    "python3 -V",
    # redirection — rejected everywhere
    "cat f > out",
    "echo hi > f",
    "ls > /dev/null",
    "ls 2>/dev/null",
    "ls >> f",
    "head << EOF",
    "sort < f",
    "cat f 2>&1",
    # non-connector operators
    "ls & rm f",
    "(ls)",
    "ls | (rm f)",
    # unsafe second segment
    "cat f; rm f",
    "ls && rm f",
    "git status || rm f",
    # substitution / expansion / newlines — hard pre-parse rejects
    "git log $(rm x)",
    "git log `rm x`",
    "echo $HOME",
    "echo ${PATH}",
    "ls\nrm f",
    "ls\rrm f",
    # unparseable quoting
    'grep "unbalanced',
    # empty / whitespace
    "",
    "   ",
    # empty segments around connectors
    "| ls",
    "ls &&",
    "ls ; ; ls",
    # sort output flags
    "sort -o /etc/x f",
    "sort -o/etc/x f",
    "sort --output=/etc/x f",
    "sort --output /etc/x f",
    # find write/exec actions
    "find . -delete",
    "find . -exec rm {} \\;",
    "find . -execdir rm {} \\;",
    "find . -ok rm {} \\;",
    "find . -okdir rm {} \\;",
    "find . -fls out",
    "find . -fprint out",
    "find . -fprintf out fmt",
    "find . -fprint0 out",
    # git: non-read-only subcommands
    "git push",
    "git checkout main",
    "git commit -m x",
    "git reset --hard",
    "git",
    "git --no-pager",
    # git: output redirection flags anywhere
    "git log --output=/tmp/x",
    "git log --output /tmp/x",
    "git -o log",
    "git log -o",
    # git branch: ref-mutating flags
    "git branch -D main",
    "git branch -d main",
    "git branch -m old new",
    "git branch -M old new",
    "git branch -c a b",
    "git branch -C a b",
    "git branch --delete main",
    "git branch --move a b",
    "git branch --copy a b",
    "git branch --set-upstream-to=origin/main",
    # git remote / tag: anything beyond listing
    "git remote add origin url",
    "git remote remove origin",
    "git tag v1",
    "git tag -d v1",
    "git tag --list 'v*'",
]


@pytest.mark.parametrize("command", SAFE)
def test_read_only_commands_are_allowed(command: str) -> None:
    assert is_read_only_command(command) is True


@pytest.mark.parametrize("command", UNSAFE)
def test_non_read_only_commands_are_rejected(command: str) -> None:
    assert is_read_only_command(command) is False
