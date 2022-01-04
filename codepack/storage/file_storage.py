from codepack.storage import Storage


class FileStorage(Storage):
    def __init__(self, obj=None, path='./'):
        super().__init__(obj=obj)
        self.path = None
        self.init(path=path)

    def init(self, path='./'):
        self.path = path
