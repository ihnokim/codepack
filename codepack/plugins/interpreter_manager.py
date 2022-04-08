from codepack.plugins.manager import Manager
import os
from typing import Any, Union, Optional, TypeVar
import json
import subprocess


CompletedProcess = TypeVar('CompletedProcess', bound='subprocess.CompletedProcess')


class InterpreterManager(Manager):
    def __init__(self, path: str, run_opt: Optional[str] = None) -> None:
        self.path = path
        if run_opt:
            self.run_opt = json.loads(run_opt)
        else:
            self.run_opt = dict()

    def get_env(self, env: str) -> str:
        return os.path.join(self.path, env, 'bin')

    def run(self, env: str, command: Optional[Union[str, list]] = None, **kwargs: Any) -> CompletedProcess:
        if len(command) > 0 and 'python' in command[0]:
            interpreter = os.path.join(self.get_env(env), command[0])
            _command = [interpreter, *command[1:]]
            return subprocess.run(_command, **kwargs, **self.run_opt)
        else:
            raise ValueError('invalid command: %s' % command)
