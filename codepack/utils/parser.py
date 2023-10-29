from typing import List


class Parser:
    @classmethod
    def parse_bool(cls, x: str) -> bool:
        if x.lower() == 'true':
            return True
        elif x.lower() == 'false':
            return False
        else:
            raise TypeError(f"'{x}' should be either 'true' or 'false'")

    @classmethod
    def parse_list(cls, x: str) -> List[str]:
        return [n.strip() for n in x.split(',')]

    @classmethod
    def parse_none(cls, x: str) -> None:
        if x.lower() in {'null', 'none', ''}:
            return None
        else:
            raise TypeError(f"'{x}' should be 'null', 'none' or an empty string")

    @classmethod
    def parse_int(cls, x: str) -> int:
        return int(x)

    @classmethod
    def parse_float(cls, x: str) -> float:
        return float(x)

    @classmethod
    def parse_tuple(cls, x: tuple) -> tuple:
        return tuple(cls.parse_list(x))

    @classmethod
    def parse_set(cls, x: tuple) -> set:
        return set(cls.parse_list(x))
