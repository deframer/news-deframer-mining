from urllib.parse import urlsplit

from news_deframer.netutil import get_root_domain


def test_get_root_domain_with_subdomain() -> None:
    url = "https://news.bbc.co.uk/articles"
    assert get_root_domain(url) == "bbc.co.uk"


def test_get_root_domain_handles_missing_scheme() -> None:
    url = "WWW.Example.COM/path"
    assert get_root_domain(url) == "example.com"


def test_get_root_domain_with_ip_address() -> None:
    assert get_root_domain("http://127.0.0.1:8080") == "127.0.0.1"


def test_get_root_domain_with_localhost() -> None:
    parsed = urlsplit("http://localhost:3000")
    assert get_root_domain(parsed) == "localhost"
