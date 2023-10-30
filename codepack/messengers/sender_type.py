from codepack.messengers.sender import Sender
from codepack.utils.type_manager import TypeManager


class SenderType(TypeManager):
    types = {'memory': Sender}
