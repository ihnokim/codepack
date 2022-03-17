from collections.abc import Callable


class Common:
    def __init__(self):
        self.variables = dict()
        self._destroy = dict()

    def __getitem__(self, item):
        return self.variables[item]

    def __getattr__(self, item):
        return self.__getitem__(item)

    def keys(self):
        return self.variables.keys()

    def values(self):
        return self.variables.values()

    def items(self):
        return self.variables.items()

    def add(self, key, value, destroy=None):
        self.variables[key] = value
        self._destroy[key] = destroy

    def remove(self, key):
        tmp = self.variables.pop(key)
        destroy = self._destroy.pop(key)
        if destroy and isinstance(destroy, Callable):
            destroy(tmp)

    def clear(self):
        keys = list(self.keys())
        for key in keys:
            self.remove(key)
