from codepack.abc import CodeBase, Storable


class Dependency(Storable):
    def __init__(self, code=None, id=None, serial_number=None, arg=None):
        super().__init__(id=id, serial_number=serial_number)
        self.code = None
        self.arg = None
        self.bind(code)
        self.set(id=id, serial_number=serial_number, arg=arg)

    def __lshift__(self, sender):
        if isinstance(sender, CodeBase):
            self.set(id=sender.id, serial_number=sender.serial_number, arg=self.arg)
        elif isinstance(sender, dict):
            self.set(id=sender['id'], serial_number=sender['serial_number'], arg=self.arg)
        else:
            raise TypeError(type(sender))
        self.code.add_dependency(self)

    def bind(self, code):
        self.code = code

    def set(self, id=None, serial_number=None, arg=None):
        self.id = id
        self.serial_number = serial_number
        self.arg = arg

    def __hash__(self):
        return hash(self.serial_number)

    def __eq__(self, other):
        if isinstance(other, type(self)):
            return self.serial_number == other.serial_number
        elif isinstance(other, dict):
            return self.serial_number == other['serial_number']
        else:
            return False

    def __str__(self):
        return '%s(arg: %s, id: %s)' % (self.__class__.__name__, self.arg, self.id)

    def __repr__(self):
        return self.__str__()

    def to_dict(self):
        return {'id': self.id, 'serial_number': self.serial_number, 'arg': self.arg}

    @classmethod
    def from_dict(cls, d, *args, **kwargs):
        return cls(id=d['id'], serial_number=d['serial_number'], arg=d['arg'])
