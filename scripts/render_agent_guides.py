"""Render both self-contained prompts from one bundled setup reference."""

import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def render(check: bool = False) -> None:
    setup = (ROOT / "src/lhp/resources/skills/lhp/references/setup.md").read_text()
    intents = {
        "agent.md": (
            "Build with Lakehouse Plumber",
            "Help me build with Lakehouse Plumber (LHP). Install and use the lhp development skill using the instructions below, then continue my project request. If I have not described a project yet, ask what pipeline I want to build after setup.",
        ),
        "learn.md": (
            "Learn Lakehouse Plumber",
            "Teach me Lakehouse Plumber (LHP) using the separate lhp-learn skill. I already know Databricks, Python and SQL. Install and read lhp-learn using the instructions below. Start with Lesson 0: explain LHP concepts and give a short description of each capability, then guide me through the sample project. Teach one checkpoint at a time, tell me which files to open and what to inspect, and stop until I explicitly say I am ready. I make the exercise edits; you handle routine commands and give hints. Save progress in .lhp/learning-progress.md so another session or coding agent can resume. If I supplied a project, use it; if I requested a feature lesson or have saved progress, start there. Offer advanced lessons when relevant. Do not load the lhp development skill as the teaching instructions.",
        ),
    }
    for name, (title, intent) in intents.items():
        skill_name = "lhp-learn" if name == "learn.md" else "lhp"
        selected_setup = setup.replace("@@SKILL_NAME@@", skill_name).replace(
            "@@SKILL_PACKAGE@@", skill_name.replace("-", "_")
        )
        content = f"# {title}\n\nCopy the complete prompt below into your coding agent. It contains its own setup instructions and does not require the agent to open this page.\n\n---\n\n{intent}\n\n{selected_setup}"
        destination = ROOT / "docs/_agent_guides" / name
        if check:
            if destination.read_text() != content:
                raise SystemExit(
                    f"Regenerate {destination} with scripts/render_agent_guides.py"
                )
        else:
            destination.write_text(content, encoding="utf-8")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    render(parser.parse_args().check)
