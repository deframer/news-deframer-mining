from __future__ import annotations

import fcntl
import os
import shutil
import tempfile
import urllib.request
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


def get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def get_model_root() -> Path:
    model_root = os.environ.get("MODEL_ROOT")
    if model_root:
        return Path(model_root)

    candidate = get_project_root() / "models"
    if candidate.exists() and not os.access(candidate, os.W_OK):
        return Path.home() / ".cache" / "news-deframer-miner" / "models"
    return candidate


def ensure_model_storage() -> None:
    root = get_model_root()
    targets = [
        root,
        root / "spacy",
        root / "spacy" / ".locks",
        root / "memolon",
        root / "memolon" / ".locks",
    ]

    for target in targets:
        try:
            target.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise RuntimeError(
                f"Failed to create model storage directory: {target}"
            ) from exc


@contextmanager
def acquire_lock(lock_path: Path) -> Iterator[None]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as lock_file:
        fcntl.flock(lock_file, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(lock_file, fcntl.LOCK_UN)


def download_url_to_path(url: str, target_path: Path, timeout: int = 60) -> None:
    target_path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        dir=target_path.parent,
        prefix=f".{target_path.name}.",
        suffix=".tmp",
    )
    os.close(fd)
    temp_path = Path(temp_name)
    try:
        with (
            urllib.request.urlopen(url, timeout=timeout) as response,
            temp_path.open("wb") as target_file,
        ):
            shutil.copyfileobj(response, target_file)
        os.replace(temp_path, target_path)
    finally:
        temp_path.unlink(missing_ok=True)


def download_wheel_to_directory(url: str, target_dir: Path, timeout: int = 60) -> None:
    target_dir.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(
        dir=target_dir.parent,
        prefix=f".{target_dir.name}.",
        suffix=".whl",
    )
    os.close(fd)
    wheel_path = Path(temp_name)
    temp_dir = Path(
        tempfile.mkdtemp(dir=target_dir.parent, prefix=f".{target_dir.name}.")
    )
    try:
        download_url_to_path(url, wheel_path, timeout=timeout)
        with zipfile.ZipFile(wheel_path) as archive:
            archive.extractall(temp_dir)
        os.replace(temp_dir, target_dir)
    finally:
        wheel_path.unlink(missing_ok=True)
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
