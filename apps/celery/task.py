from dataclasses import dataclass
from datetime import date


@dataclass
class Task:
    id: str
    platform: str
    doc_type: str
    status: str
    date_from: date
    date_to: date
