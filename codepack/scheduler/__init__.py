from codepack.scheduler.scheduler import Scheduler
from codepack.scheduler.mongo_scheduler import MongoScheduler
from codepack.utils.config import get_default_service_config
from enum import Enum
import os


class SchedulerAlias(Enum):
    MONGODB = MongoScheduler


def get_default_scheduler(config_path=None):
    config = get_default_service_config(section='scheduler', config_path=config_path)
    source = config.pop('source', None)
    supervisor = config.pop('superviosr', None)
    if 'CODEPACK_SCHEDULER_SUPERVISOR' in os.environ:
        supervisor = os.environ['CODEPACK_SCHEDULER_SUPERVISOR']
    cls = SchedulerAlias[source].value
    scheduler = cls(**config)
    if supervisor:
        if 'CODEPACK_SCHEDULER_SUPERVISOR' not in os.environ:
            os.environ['CODEPACK_SCHEDULER_SUPERVISOR'] = supervisor
        scheduler.register(scheduler.request_to_supervisor)
    return scheduler
