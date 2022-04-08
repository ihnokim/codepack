from codepack import Code, CodeSnapshot, CallbackService, Callback, Default, Worker
from codepack.storages import FileStorage
import argparse
import sys
from functools import partial


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('snapshot', metavar='SNAPSHOT', type=str, help='a JSON file of the snapshot to run')
    parser.add_argument('-c', '--callback', action='append', help='callback functions for the snapshot')
    parser.add_argument('-p', '--path', help='path to the directory including callback functions')
    parser.add_argument('-l', '--logger', help='a logger name to write logs')
    args = parser.parse_args()
    if args.logger:
        logger = Default.get_logger(args.logger)
        sys.stdout.write = partial(Worker.log, logger.info)
    else:
        logger = None
    try:
        callback_storage = FileStorage(item_type=Callback, path=args.path if args.path else '.')
        callback_service = CallbackService(storage=callback_storage)
        code_snapshot = CodeSnapshot.from_file(args.snapshot)
        code_args = code_snapshot.args
        code_kwargs = code_snapshot.kwargs
        code = Code.from_snapshot(code_snapshot)
        if args.callback:
            callbacks = list()
            for name in args.callback:
                cb = callback_service.pull(name)
                callbacks.append(cb)
            code.register_callback(callbacks)
        code(*code_args, **code_kwargs)
    except Exception as e:
        if logger:
            logger.error(e)
        else:
            print(e)
