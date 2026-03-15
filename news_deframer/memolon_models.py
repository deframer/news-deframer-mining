import os
import urllib.request
from pathlib import Path

MEMOLON_VERSION = "0.0.1"

MEMOLON_LANGUAGE_MODELS = {
    "en": f"memolon-grouped-en-{MEMOLON_VERSION}.parquet",
    "de": f"memolon-grouped-de-{MEMOLON_VERSION}.parquet",
    "es": f"memolon-grouped-es-{MEMOLON_VERSION}.parquet",
    "fr": f"memolon-grouped-fr-{MEMOLON_VERSION}.parquet",
    "it": f"memolon-grouped-it-{MEMOLON_VERSION}.parquet",
    "pt": f"memolon-grouped-pt-{MEMOLON_VERSION}.parquet",
    "nl": f"memolon-grouped-nl-{MEMOLON_VERSION}.parquet",
    "ru": f"memolon-grouped-ru-{MEMOLON_VERSION}.parquet",
    "da": f"memolon-grouped-da-{MEMOLON_VERSION}.parquet",
}


def get_project_root():
    """Returns the project root directory."""
    return Path(__file__).parent.parent


def check_url(url):
    """Check if a URL exists (HEAD request) to verify model existence."""
    try:
        req = urllib.request.Request(url, method="HEAD")
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.status == 200
    except Exception:
        return False


def download_file(url, target_dir, filename):
    """Download a file to a target directory."""
    target_path = target_dir / filename
    if target_path.exists():
        print(f"Model {filename} already exists in {target_dir}.")
        return

    print(f"Downloading model from: {url}...")
    try:
        urllib.request.urlretrieve(url, target_path)
        print(f"Successfully downloaded {filename} to {target_dir}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")


def install_models():
    # Example ENV: MEMOLON_MODELS="en de"
    requested = os.environ.get("MEMOLON_MODELS", "en de").split()

    if not requested:
        print("Info: Set MEMOLON_MODELS to install languages (e.g., 'en de').")
        return

    root_dir = get_project_root()
    target_dir = root_dir / "memolon"
    target_dir.mkdir(exist_ok=True)

    for lang in requested:
        if lang in MEMOLON_LANGUAGE_MODELS:
            model_filename = MEMOLON_LANGUAGE_MODELS[lang]
            url = f"https://github.com/deframer/memolon-parquet/releases/download/v{MEMOLON_VERSION}/{model_filename}"

            if check_url(url):
                download_file(url, target_dir, model_filename)
            else:
                print(f"Could not find a valid Memolon model for '{lang}' at {url}.")
        else:
            print(f"Language '{lang}' is not supported for Memolon models.")
