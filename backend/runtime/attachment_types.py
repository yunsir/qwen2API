from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class NormalizedAttachment:
    file_id: str
    filename: str = ""
    content_type: str = "application/octet-stream"
    source: str = "upload"
