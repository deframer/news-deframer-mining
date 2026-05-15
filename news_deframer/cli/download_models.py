from news_deframer.spacy_models import install_models as install_spacy_models
from news_deframer.memolon_models import install_models as install_memolon_models


def main() -> int:
    install_spacy_models()
    install_memolon_models()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
