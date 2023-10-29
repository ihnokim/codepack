from typing import Optional, Callable


class Dependency:
    def __init__(self,
                 on_update: Optional[Callable[[str], None]] = None) -> None:
        self.on_update = on_update

    def __or__(self, param: str) -> None:
        self.param = param
        self.on_update(param)
