def inform_supervisor_of_termination(x, supervisor):
    import requests
    from codepack.plugins.supervisor import Supervisor
    if x['state'] == 'TERMINATED':
        if isinstance(supervisor, str):
            requests.get(supervisor + '/organize/%s' % x['serial_number'])
        elif isinstance(supervisor, Supervisor):
            supervisor.organize(x['serial_number'])
