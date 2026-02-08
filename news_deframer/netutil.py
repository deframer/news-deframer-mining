"""Network-related helpers."""

from __future__ import annotations

from functools import lru_cache
import ipaddress
from typing import Union
from urllib.parse import SplitResult, urlsplit

from publicsuffix2 import get_sld  # type: ignore[import-untyped]


@lru_cache(maxsize=2048)
def get_root_domain(url: Union[str, SplitResult]) -> str:
    """Return the effective top-level domain plus one (eTLD+1).

    Falls back to the hostname itself when public suffix extraction fails,
    mirroring the behavior of the original Go helper.
    """

    if isinstance(url, SplitResult):
        host = url.hostname or ""
    else:
        if not url:
            return ""
        value = url if "://" in url else f"//{url}"
        parsed = urlsplit(value)
        host = parsed.hostname or ""

    if not host:
        return ""

    if host == "localhost":
        return host

    try:
        ipaddress.ip_address(host)
    except ValueError:
        pass
    else:
        return host

    try:
        domain = get_sld(host)
    except Exception:
        domain = None

    return domain or host


@lru_cache(maxsize=2048)
def get_base_domain_name(url: Union[str, SplitResult]) -> str:
    """Return the base domain name (eTLD+1 without the TLD).

    e.g. 'www.example.com' -> 'example'
         'news.bbc.co.uk' -> 'bbc'
    """
    root = get_root_domain(url)
    if not root:
        return ""

    if root == "localhost":
        return root

    try:
        ipaddress.ip_address(root)
    except ValueError:
        pass
    else:
        return root

    return root.split(".", 1)[0]


def flush_domain_cache() -> None:
    """Clear the internal caches for domain lookups."""
    get_root_domain.cache_clear()
    get_base_domain_name.cache_clear()
