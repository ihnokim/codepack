from codepack import Code, CodeSnapshot, Default, Worker
import argparse
import sys
from functools import partial


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('snapshot', metavar='SNAPSHOT', type=str, help='a JSON file of the snapshot to run')
    parser.add_argument('-l', '--logger', help='a logger name to write logs')
    args = parser.parse_args()
    if args.logger:
        logger = Default.get_logger(args.logger)
        sys.stdout.write = partial(Worker.log, logger.info)
    else:
        logger = None
    try:
        code_snapshot = CodeSnapshot.from_file(args.snapshot)
        code_args = code_snapshot.args
        code_kwargs = code_snapshot.kwargs
        code = Code.from_snapshot(code_snapshot)
        code(*code_args, **code_kwargs)
    except Exception as e:
        if logger:
            logger.error(e)
        else:
            print(e)
