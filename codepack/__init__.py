import codepack.storages
import codepack.interfaces


__version__ = '0.5.0'


__lazy_imports__ = {
    'Code': 'codepack.code',
    'CodePack': 'codepack.codepack',
    'Arg': 'codepack.arg',
    'ArgPack': 'codepack.argpack',
    'Config': 'codepack.utils.config.config',
    'Default': 'codepack.utils.config.default',
    'Alias': 'codepack.utils.config.alias',
    'Looper': 'codepack.utils.looper',
    'Common': 'codepack.utils.common',
    'DependencyBag': 'codepack.plugins.dependency_bag',
    'DependencyMonitor': 'codepack.plugins.dependency_monitor',
    'StorageService': 'codepack.plugins.storage_service',
    'SnapshotService': 'codepack.plugins.snapshot_service',
    'DeliveryService': 'codepack.plugins.delivery_service',
    'Dependency': 'codepack.plugins.dependency',
    'Delivery': 'codepack.plugins.delivery',
    'Callback': 'codepack.plugins.callback',
    'Snapshot': 'codepack.plugins.snapshots.snapshot',
    'CodeSnapshot': 'codepack.plugins.snapshots.code_snapshot',
    'CodePackSnapshot': 'codepack.plugins.snapshots.codepack_snapshot',
    'State': 'codepack.plugins.state',
    'Worker': 'codepack.plugins.worker',
    'Supervisor': 'codepack.plugins.supervisor',
    'DockerManager': 'codepack.plugins.docker_manager',
    'InterpreterManager': 'codepack.plugins.interpreter_manager',
    'Scheduler': 'codepack.plugins.scheduler',
    'JobStore': 'codepack.plugins.jobstore',
    'StorableJob': 'codepack.plugins.storable_job',
}


def __getattr__(name):
    path = __lazy_imports__.get(name)
    if not path:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    import operator
    without_prefix = path.split('.', 1)[-1]
    getter = operator.attrgetter(f'{without_prefix}.{name}')
    val = getter(__import__(path))
    globals()[name] = val
    return val
