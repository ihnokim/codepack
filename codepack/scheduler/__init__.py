from codepack.scheduler.scheduler import Scheduler
from codepack.scheduler.mongo_scheduler import MongoScheduler
from codepack.config import Config
from enum import Enum
import os


class SchedulerAlias(Enum):
    MONGODB = MongoScheduler


def get_default_scheduler(config_path=None):
    config = Config(config_path=config_path)
    storage_config = config.get_storage_config(section='scheduler')
    source = storage_config.pop('source', None)
    supervisor = storage_config.pop('supervisor', None)
    if 'CODEPACK_SCHEDULER_SUPERVISOR' in os.environ:
        supervisor = os.environ['CODEPACK_SCHEDULER_SUPERVISOR']
    cls = SchedulerAlias[source].value
    scheduler = cls(**storage_config)
    if supervisor:
        if 'CODEPACK_SCHEDULER_SUPERVISOR' not in os.environ:
            os.environ['CODEPACK_SCHEDULER_SUPERVISOR'] = supervisor
        scheduler.register(scheduler.request_to_supervisor)
    return scheduler
