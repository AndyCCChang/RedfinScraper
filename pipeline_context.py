from datetime import datetime
import os
from pathlib import Path
from typing import Optional, Tuple


RUNS_DIR = Path("runs")
CURRENT_RUN_FILE = RUNS_DIR / ".current_run"
LATEST_RUN_LINK = RUNS_DIR / "latest"
LATEST_REPORT_LINK = Path("latest_report.html")
RUN_DIR_ENV = "REDFIN_RUN_OUTPUT_DIR"
RUN_TS_ENV = "REDFIN_RUN_TIMESTAMP"


def _timestamp_now() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _timestamp_from_run_dir(run_dir: Path) -> str:
    name = run_dir.name
    if name.startswith("run_"):
        return name[4:]
    return name


def _update_latest_pointer(run_dir: Path) -> None:
    if LATEST_RUN_LINK.exists() or LATEST_RUN_LINK.is_symlink():
        if LATEST_RUN_LINK.is_dir() and not LATEST_RUN_LINK.is_symlink():
            # Avoid deleting a real directory unexpectedly.
            return
        LATEST_RUN_LINK.unlink()

    try:
        LATEST_RUN_LINK.symlink_to(run_dir.name, target_is_directory=True)
    except OSError:
        fallback = RUNS_DIR / "latest.txt"
        fallback.write_text(str(run_dir), encoding="utf-8")


def update_latest_report_pointer(run_dir: Path, timestamp: str) -> None:
    report_path = (run_dir / f"report_{timestamp}.html").resolve()
    if not report_path.exists():
        return

    if LATEST_REPORT_LINK.exists() or LATEST_REPORT_LINK.is_symlink():
        if LATEST_REPORT_LINK.is_dir() and not LATEST_REPORT_LINK.is_symlink():
            return
        LATEST_REPORT_LINK.unlink()

    try:
        LATEST_REPORT_LINK.symlink_to(report_path)
    except OSError:
        fallback = Path("latest_report.txt")
        fallback.write_text(str(report_path), encoding="utf-8")


def ensure_run_context(create: bool = False) -> Tuple[Path, str]:
    env_run_dir = os.environ.get(RUN_DIR_ENV)
    if env_run_dir:
        run_dir = Path(env_run_dir)
        run_dir.mkdir(parents=True, exist_ok=True)
        RUNS_DIR.mkdir(parents=True, exist_ok=True)
        _update_latest_pointer(run_dir)
        timestamp = os.environ.get(RUN_TS_ENV) or _timestamp_from_run_dir(run_dir)
        return run_dir, timestamp

    if CURRENT_RUN_FILE.exists():
        saved_dir = Path(CURRENT_RUN_FILE.read_text(encoding="utf-8").strip())
        if saved_dir.exists():
            return saved_dir, _timestamp_from_run_dir(saved_dir)

    if not create:
        raise FileNotFoundError("No active run context found.")

    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = _timestamp_now()
    run_dir = RUNS_DIR / f"run_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    CURRENT_RUN_FILE.write_text(str(run_dir), encoding="utf-8")
    _update_latest_pointer(run_dir)
    return run_dir, timestamp


def start_new_run_context() -> Tuple[Path, str]:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = _timestamp_now()
    run_dir = RUNS_DIR / f"run_{timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    CURRENT_RUN_FILE.write_text(str(run_dir), encoding="utf-8")
    _update_latest_pointer(run_dir)
    return run_dir, timestamp


def output_path(stem: str, suffix: str, create: bool = False) -> Path:
    run_dir, timestamp = ensure_run_context(create=create)
    return run_dir / f"{stem}_{timestamp}{suffix}"


def resolve_input_path(stem: str, suffix: str) -> Path:
    try:
        run_dir, timestamp = ensure_run_context(create=False)
        candidate = run_dir / f"{stem}_{timestamp}{suffix}"
        if candidate.exists():
            return candidate

        matches = sorted(run_dir.glob(f"{stem}_*{suffix}"))
        if matches:
            return matches[-1]
    except FileNotFoundError:
        pass

    legacy_path = Path(f"{stem}{suffix}")
    if legacy_path.exists():
        return legacy_path

    matches = sorted(RUNS_DIR.glob(f"run_*/{stem}_*{suffix}"))
    if matches:
        return matches[-1]

    return legacy_path


def current_run_dir() -> Optional[Path]:
    try:
        run_dir, _ = ensure_run_context(create=False)
        return run_dir
    except FileNotFoundError:
        return None
