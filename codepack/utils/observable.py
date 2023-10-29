from typing import Any, Optional, Dict
from codepack.utils.observer import Observer


class Observable:
    def __init__(self):
        self.observers: Dict[str, Observer] = dict()

    def register_observer(self, id: str, observer: Observer) -> None:
        if id in self.observers.keys():
            raise ValueError(f"'{id}' already exists")
        else:
            self.observers[id] = observer

    def get_observer(self, id: str) -> Optional[Observer]:
        return self.observers.get(id, None)

    def unregister_observer(self, id: str) -> None:
        self.observers.pop(id)

    def notify_observers(self, **kwargs: Any) -> None:
        for observer in self.observers.values():
            # TODO: add retry and pop logic
            observer.update(observable=self, **kwargs)
