"""Shared helpers for all portal pages."""
import subprocess
import streamlit as st


def run_command(args: list[str], cwd: str | None = None) -> int:
    """Stream subprocess output live into a Streamlit code block."""
    log_area = st.empty()
    lines = []
    with subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, cwd=cwd,
    ) as proc:
        for line in proc.stdout:
            lines.append(line.rstrip())
            log_area.code("\n".join(lines[-60:]), language="log")
    return proc.returncode
