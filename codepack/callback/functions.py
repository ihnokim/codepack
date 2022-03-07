def inform_supervisor_of_termination(self, x):
    import requests
    from codepack.employee.supervisor import Supervisor
    if x['state'] == 'TERMINATED':
        if isinstance(self.supervisor, str):
            requests.get(self.supervisor + '/organize/%s' % x['serial_number'])
        elif isinstance(self.supervisor, Supervisor):
            self.supervisor.organize(x['serial_number'])
