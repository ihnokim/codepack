from codepack.code import Code
from codepack.codepack import CodePack
from codepack.utils.config.default import Default
from codepack.plugins.snapshots.code_snapshot import CodeSnapshot
from codepack.plugins.employee import Employee
from codepack.arg import Arg
from codepack.argpack import ArgPack
from typing import TypeVar, Optional, Union


Messenger = TypeVar('Messenger', bound='codepack.storages.messenger.Messenger')
SnapshotService = TypeVar('SnapshotService', bound='codepack.plugins.snapshot_service.SnapshotService')


class Supervisor(Employee):
    def __init__(self, messenger: Messenger, snapshot_service: Optional[SnapshotService] = None) -> None:
        super().__init__(messenger=messenger)
        self.snapshot_service =\
            snapshot_service if snapshot_service else Default.get_service('code_snapshot', 'snapshot_service')

    def run_code(self, code: Code, args: Optional[tuple] = None, kwargs: Optional[Union[Arg, dict]] = None) -> str:
        if isinstance(code, Code):
            code.update_state('READY')
            if kwargs and isinstance(kwargs, Arg):
                _kwargs = kwargs.to_dict()
            else:
                _kwargs = kwargs
            self.messenger.send(item=code.to_snapshot(args=args, kwargs=_kwargs).to_dict())
        else:
            raise TypeError(type(code))  # pragma: no cover
        return code.serial_number

    def run_codepack(self, codepack: CodePack, argpack: Optional[Union[ArgPack, dict]] = None) -> str:
        if isinstance(codepack, CodePack):
            codepack.save_snapshot(argpack=argpack)
            codepack.init_code_state(state='READY', argpack=argpack)
            for id, code in codepack.codes.items():
                _kwargs = argpack[id] if id in argpack else None
                self.run_code(code=code, kwargs=_kwargs)
        else:
            raise TypeError(type(codepack))  # pragma: no cover
        return codepack.serial_number

    def organize(self, serial_number: Optional[str] = None) -> None:
        for snapshot in self.snapshot_service.search(key='state', value='WAITING'):
            resolved = True
            dependent_serial_numbers = [snapshot['serial_number'] for snapshot in snapshot['dependency']]
            dependencies = self.snapshot_service.load(dependent_serial_numbers, projection=['state'])
            known_dependent_serial_numbers_set = {dependency['serial_number'] for dependency in dependencies}
            for dependent_serial_number in dependent_serial_numbers:
                if dependent_serial_number not in known_dependent_serial_numbers_set:
                    dependencies.append({'serial_number': dependent_serial_number, 'state': 'UNKNOWN'})
            if serial_number and serial_number not in known_dependent_serial_numbers_set:
                continue
            for dependency in dependencies:
                if dependency['state'] != 'TERMINATED':
                    resolved = False
                    break
            if resolved:
                code_snapshot = CodeSnapshot.from_dict(snapshot)
                code = Code.from_snapshot(code_snapshot)
                self.run_code(code=code, args=code_snapshot.args, kwargs=code_snapshot.kwargs)
