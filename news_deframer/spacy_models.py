import importlib.util
import os
import subprocess
import sys
import urllib.request

SPACY_VERSION = "3.8.0"

SPACY_LANGUAGE_MODELS = {
    "en": "en_core_web_sm",
    "de": "de_core_news_sm",
    "es": "es_core_news_sm",
    "fr": "fr_core_news_sm",
    "it": "it_core_news_sm",
    "pt": "pt_core_news_sm",
    "nl": "nl_core_news_sm",
    "pl": "pl_core_news_sm",
    "ru": "ru_core_news_sm",
}


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
        if known := SPACY_LANGUAGE_MODELS.get(lang):
            candidates = [known]
        else:
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
            print(
                f"Could not find a valid spaCy model for '{lang}' (checked {candidates})."
            )
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

#
# TODO: maybe try it this way
#

# import spacy
# import requests
# import tarfile
# import os

# # 1. Define URL and paths
# model_url = "https://example.com/path/to/your-model.tar.gz"
# save_path = "model.tar.gz"
# extract_dir = "./local_model"

# # 2. Download the file
# response = requests.get(model_url, stream=True)
# if response.status_code == 200:
#     with open(save_path, 'wb') as f:
#         f.write(response.raw.read())

# # 3. Extract the file
# with tarfile.open(save_path, "r:gz") as tar:
#     tar.extractall(path=extract_dir)

# # 4. Load from the local directory
# # Note: You must point to the folder containing 'config.cfg'
# # often this is a subfolder inside the extracted directory, e.g.:
# model_path = os.path.join(extract_dir, "en_core_web_sm-3.7.1")

# nlp = spacy.load(model_path)
