from dataclasses import dataclass
from datetime import datetime
from typing import Dict, List, Iterable

from flask import current_app


@dataclass
class Report:
    _id: str
    platform: str
    doc_type: str
    state: str
    date_from: datetime
    date_to: datetime
    files: Dict[str, str]
    rows: List[dict]

    @property
    def id(self):
        return self._id

    @property
    def rows_for_pd(self) -> Iterable[dict]:
        for row in self.rows:
            for rep in row['reports']:
                yield {**{key: value for key, value in row.items() if key != 'reports'}, **rep}


def get_reports_url():
    return current_app.config['CORE_REPORTS_URL']
