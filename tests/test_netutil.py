from urllib.parse import urlsplit

from news_deframer.netutil import (
    flush_domain_cache,
    get_base_domain_name,
    get_root_domain,
)


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


def test_get_base_domain_name_simple() -> None:
    assert get_base_domain_name("www.example.com") == "example"


def test_get_base_domain_name_multipart_tld() -> None:
    assert get_base_domain_name("http://news.bbc.co.uk/story") == "bbc"


def test_get_base_domain_name_ip() -> None:
    assert get_base_domain_name("http://127.0.0.1:8080") == "127.0.0.1"


def test_get_base_domain_name_localhost() -> None:
    assert get_base_domain_name("http://localhost:3000") == "localhost"


def test_flush_domain_cache() -> None:
    # Ensure it runs without error and cache behavior is consistent
    assert get_root_domain("www.example.com") == "example.com"
    flush_domain_cache()
    assert get_root_domain("www.example.com") == "example.com"
    assert get_base_domain_name("www.example.com") == "example"
