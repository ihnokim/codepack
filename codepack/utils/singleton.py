class Singleton(object):
    @classmethod
    def __new__(cls, *args, **kwargs):
        if not hasattr(cls, "__instance__"):
            cls.__instance__ = super().__new__(cls)
        return cls.__instance__
