from typing import Union, TypeVar


Supervisor = TypeVar('Supervisor', bound='codepack.plugin.supervisor.Supervisor')


def inform_supervisor_of_termination(x: dict, supervisor: Union[Supervisor, str]):
    import requests
    from codepack.plugin.supervisor import Supervisor
    if x['state'] == 'TERMINATED':
        if isinstance(supervisor, str):
            requests.get(supervisor + '/organize/%s' % x['serial_number'])
        elif isinstance(supervisor, Supervisor):
            supervisor.organize(x['serial_number'])
