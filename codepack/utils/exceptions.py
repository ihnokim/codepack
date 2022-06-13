class UnknownState(Exception):
    pass


class NewState(Exception):
    pass


class ReadyState(Exception):
    pass


class WaitingState(Exception):
    pass


class RunningState(Exception):
    pass


class TerminateState(Exception):
    pass


class ErrorState(Exception):
    pass
