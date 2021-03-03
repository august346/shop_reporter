import uuid

from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import UUID

from db import db, ChoiceType


class Task(db.Model):
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    platform = Column(ChoiceType({"wb": "wb"}), nullable=False)
    doc_type = Column(ChoiceType({"fin_monthly": "fin_monthly"}), nullable=False)
    status = Column(ChoiceType({"init": "init", "process": "process", "complete": "complete"}), default="init")

    def as_dict(self):
        return dict(
            id=str(self.id),
            platform=self.platform,
            doc_type=self.doc_type,
            status=self.status,
        )
