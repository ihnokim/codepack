class Common:
    def __init__(self):
        self.variables = dict()

    def __getitem__(self, item):
        return self.variables[item]

    def __setitem__(self, key, value):
        self.variables[key] = value

    def __getattr__(self, item):
        return self.__getitem__(item)

    def keys(self):
        return self.variables.keys()

    def values(self):
        return self.variables.values()

    def items(self):
        return self.variables.items()
