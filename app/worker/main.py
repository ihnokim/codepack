from codepack.config import Default


if __name__ == '__main__':
    worker = Default.get_employee('worker')
    worker.start()
    worker.stop()
