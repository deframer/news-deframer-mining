"""Network-related helpers."""

from __future__ import annotations

import ipaddress
from typing import Union
from urllib.parse import SplitResult, urlsplit

from publicsuffix2 import get_sld  # type: ignore[import-untyped]


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
