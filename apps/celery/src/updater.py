from typing import List, Type

from src.upd_getter import UpdGetter, WbNameGetter


class Updater:
    getters: List[Type[UpdGetter]] = None

    def __init__(self, _id: str):
        self.id = _id

    def __call__(self):
        if self.getters is None:
            raise NotImplementedError

        return dict(map(
            lambda getter: getter(self.id)(),
            self.getters
        ))


class WbUpdater(Updater):
    getters = [WbNameGetter]
