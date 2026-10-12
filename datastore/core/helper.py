from __future__ import annotations

import os
import re
import unicodedata
from typing import Any
from urllib.parse import quote, unquote, urlparse


def parse_authorization_header(authorization: str | None) -> str | None:
    """Return the bare token from an `Authorization` header value, or None.

    Accepts the value verbatim — CKAN api_keys are passed as the bare token,
    no `Bearer ` prefix. Empty / whitespace-only values become `None`.
    """
    if authorization is None:
        return None
    token = authorization.strip()
    return token or None


# Characters no common OS allows in a filename, plus control characters
# (which would also break the Content-Disposition header).
_UNSAFE_FILENAME_CHARS = re.compile(r'[\\/:*?"<>|\x00-\x1f\x7f]')


def download_filename_base(resource: dict[str, Any] | None, ext: str) -> str | None:
    """The CKAN resource's `download_filename`, ready to name a dump with
    extension `ext`; None when it is unset, so callers keep their default.

    The value is meant to be a bare name, but a trailing extension that
    matches the download (`Flows.csv` for csv, or `Flows.csv` /
    `Flows.csv.gz` for gzip) is dropped so it isn't doubled up.
    """
    value = (resource or {}).get("download_filename")
    if not isinstance(value, str):
        return None
    name = _UNSAFE_FILENAME_CHARS.sub("", value).strip()
    for suffix in (f".{ext}", f".{ext.split('.')[0]}"):
        if name.lower().endswith(suffix.lower()) and len(name) > len(suffix):
            name = name[: -len(suffix)].rstrip()
            break
    # Nothing left but dots / spaces → no usable name.
    if not name.strip(". "):
        return None
    return name


_COMPRESSION_EXTENSIONS = (".gz", ".bz2", ".xz", ".zst")


def uploaded_filename_base(url: str | None) -> str | None:
    """Filename from an upload's URL without its extension
    (`…/download/my-data.csv` → `my-data`); None when there is none."""
    if not url:
        return None
    name = unquote(urlparse(url).path.rsplit("/", 1)[-1])
    stem, ext = os.path.splitext(name)
    if ext.lower() in _COMPRESSION_EXTENSIONS:
        stem = os.path.splitext(stem)[0]
    stem = _UNSAFE_FILENAME_CHARS.sub("", stem).strip()
    if not stem.strip(". "):
        return None
    return stem


def dump_filename_base(resource: dict[str, Any] | None, ext: str) -> str | None:
    """Name for a dump of `resource` with extension `ext`, or None to keep
    the default (the resource id). Uploads are named after the uploaded
    file in their `url`; other resources use `download_filename`."""
    if (resource or {}).get("url_type") == "upload":
        return uploaded_filename_base((resource or {}).get("url"))
    return download_filename_base(resource, ext)


def content_disposition(filename: str) -> str:
    """`attachment` header naming `filename`. Non-ASCII names add an RFC
    6266 `filename*` (UTF-8) next to an ASCII fallback; ASCII names keep
    the plain form."""
    try:
        filename.encode("ascii")
    except UnicodeEncodeError:
        fallback = unicodedata.normalize("NFKD", filename).encode("ascii", "ignore").decode()
        fallback = fallback.replace('"', "").replace("\\", "") or "download"
        return f"attachment; filename=\"{fallback}\"; filename*=UTF-8''{quote(filename, safe='')}"
    return f'attachment; filename="{filename}"'
