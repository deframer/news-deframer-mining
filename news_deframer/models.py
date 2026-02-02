import importlib.util
import os
import subprocess
import sys
import urllib.request

SPACY_VERSION = "3.8.0"


def ensure_pip():
    """Ensure pip is installed, as some venvs (like uv) might exclude it."""
    if importlib.util.find_spec("pip") is None:
        print("pip module not found. Bootstrapping pip via ensurepip...")
        try:
            subprocess.check_call(
                [sys.executable, "-m", "ensurepip", "--upgrade", "--default-pip"]
            )
        except subprocess.CalledProcessError as e:
            print(f"Failed to bootstrap pip: {e}")
            sys.exit(1)


def check_url(url):
    """Check if a URL exists (HEAD request) to verify model existence."""
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status == 200
    except Exception:
        return False


def install_models():
    # Example ENV: SPACY_MODELS="en de"
    requested = os.environ.get("SPACY_MODELS", "").split()

    if not requested:
        print("Info: Set SPACY_MODELS to install additional languages.")
        return

    pip_checked = False
    for lang in requested:
        # Construct potential model names.
        # Most languages use 'news', English uses 'web'.
        candidates = [f"{lang}_core_news_sm", f"{lang}_core_web_sm"]

        # 1. Check if any candidate is already installed locally
        installed = False
        for model_name in candidates:
            if importlib.util.find_spec(model_name):
                print(f"Model {model_name} already installed.")
                installed = True
                break

        if installed:
            continue

        # 2. If not installed, find the valid one on GitHub
        found_url = None
        for model_name in candidates:
            url = f"https://github.com/explosion/spacy-models/releases/download/{model_name}-{SPACY_VERSION}/{model_name}-{SPACY_VERSION}-py3-none-any.whl"
            if check_url(url):
                found_url = url
                break

        if not found_url:
            print(f"Could not find a valid spaCy model for '{lang}' (checked {candidates}).")
            sys.exit(1)

        if not pip_checked:
            ensure_pip()
            pip_checked = True

        print(f"Downloading model from: {found_url}...")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", found_url])
        except subprocess.CalledProcessError as e:
            print(f"Failed to download {found_url}: {e}")
            sys.exit(1)