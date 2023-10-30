from codepack.messengers.receiver import Receiver
from codepack.utils.type_manager import TypeManager


class ReceiverType(TypeManager):
    types = {'memory': Receiver}
