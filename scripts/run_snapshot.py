from codepack import Code
from codepack.snapshot import CodeSnapshot
import argparse
from codepack.service import CallbackService
from codepack.storage import FileStorage
from codepack.callback import Callback


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('snapshot', metavar='SNAPSHOT', type=str, help='a JSON file of the snapshot to run')
    parser.add_argument('-c', '--callback', action='append', help='callback functions for the snapshot')
    parser.add_argument('-p', '--path', help='path to the directory including callback functions')
    args = parser.parse_args()
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
