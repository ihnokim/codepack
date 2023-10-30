from codepack.jobs.states.unknown_state import UnknownState
from codepack.jobs.states.new_state import NewState
from codepack.jobs.states.checking_state import CheckingState
from codepack.jobs.states.terminated_state import TerminatedState
from codepack.jobs.states.ready_state import ReadyState
from codepack.jobs.states.running_state import RunningState
from codepack.jobs.states.error_state import ErrorState
from codepack.jobs.states.waiting_state import WaitingState
from codepack.jobs.states.async_unknown_state import AsyncUnknownState
from codepack.jobs.states.async_new_state import AsyncNewState
from codepack.jobs.states.async_checking_state import AsyncCheckingState
from codepack.jobs.states.async_terminated_state import AsyncTerminatedState
from codepack.jobs.states.async_ready_state import AsyncReadyState
from codepack.jobs.states.async_running_state import AsyncRunningState
from codepack.jobs.states.async_error_state import AsyncErrorState
from codepack.jobs.states.async_waiting_state import AsyncWaitingState
from codepack.jobs.states.state import State
from typing import List, Type, Union


sync_states = [UnknownState,
               NewState,
               CheckingState,
               WaitingState,
               ReadyState,
               RunningState,
               ErrorState,
               TerminatedState]
async_states = [AsyncUnknownState,
                AsyncNewState,
                AsyncCheckingState,
                AsyncWaitingState,
                AsyncReadyState,
                AsyncRunningState,
                AsyncErrorState,
                AsyncTerminatedState]


class States:
    @classmethod
    def get_final_state(cls, states: List[Union[State, Type[State]]]) -> Type[State]:
        state_names = [state.get_name() for state in sync_states]
        is_terminated = True
        final_state_name = UnknownState.get_name()
        for state in states:
            state_name = state.get_name()
            if state_name != TerminatedState.get_name():
                is_terminated = False
                if state_name == ErrorState.get_name():
                    return ErrorState
                if state_names.index(final_state_name) < state_names.index(state_name):
                    final_state_name = state_name
        if is_terminated:
            return TerminatedState
        return getattr(cls, final_state_name)


class AsyncStates(States):
    @classmethod
    def get_final_state(cls, states: List[Union[State, Type[State]]]) -> Type[State]:
        final_state_name = States.get_final_state(states=states).get_name()
        for state in async_states:
            if state.get_name() == final_state_name:
                return state
        return AsyncUnknownState


for state_type in sync_states:
    state = state_type()
    setattr(States, state.get_name(), state)

for state_type in async_states:
    state = state_type()
    setattr(AsyncStates, state.get_name(), state)
