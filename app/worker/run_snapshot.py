from codepack import Code
from codepack.snapshot import CodeSnapshot
import sys


if __name__ == "__main__":
    code_snapshot = CodeSnapshot.from_file(sys.argv[1])
    code_args = code_snapshot.args
    code_kwargs = code_snapshot.kwargs
    code = Code.from_snapshot(code_snapshot)
    code(*code_args, **code_kwargs)
