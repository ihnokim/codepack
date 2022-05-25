from codepack import Default


if __name__ == '__main__':
    worker = None
    try:
        worker = Default.get_employee('worker')
        worker.start()
    finally:
        if worker:
            worker.stop()
