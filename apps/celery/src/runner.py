from datetime import date

from src.task import Task

__all__ = ['run']


class Runner:
    id_key = None
    rows_getter = None
    post_runner = None

    @staticmethod
    def generate(task: Task) -> None:
        dict(...)[task.platform](task.date_from, task.date_to).run()

    def __init__(self, date_from: date, date_to: date):
        self.date_from = date_from
        self.date_to = date_to

    def run(self):
        ids = []

        for row in self.rows:
            ids.append(row[self.id_key])
            ...     # TODO insert rows

        updates = self.post_runner(*ids)
        ...     # TODO update rows

    @property
    def rows(self):
        self.id_key = yield from self.rows_getter(self.date_from, self.date_to)


run = Runner.generate
