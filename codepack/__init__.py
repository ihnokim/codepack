import sys
from typing import Optional, Callable
import codepack.storages as storages
import codepack.interfaces as interfaces


__version__ = '0.7.0'


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


__required__ = {
    'Code', 'CodePack', 'Arg', 'ArgPack', 'Config', 'Default', 'Alias', 'Looper', 'Common', 'DependencyBag',
    'DependencyMonitor', 'StorageService', 'SnapshotService', 'DeliveryService', 'Dependency', 'Delivery',
    'Callback', 'Snapshot', 'CodeSnapshot', 'CodePackSnapshot', 'State',
}


def __getattr__(name: str) -> type:
    return _import_one(module_name=__name__, module_map=__lazy_imports__, name=name)


def _import(name: str, path: str, import_function: Optional[Callable] = None) -> type:
    import operator
    _import_function = import_function if import_function else __import__
    without_prefix = path.split('.', 1)[-1]
    getter = operator.attrgetter(f'{without_prefix}.{name}')
    val = getter(_import_function(path))
    globals()[name] = val
    return val


def _import_one(module_name: str, module_map: dict, name: str) -> type:
    path = module_map.get(name)
    if not path:
        raise AttributeError(f"module {module_name!r} has no attribute {name!r}")
    return _import(path=path, name=name)


def _import_all(module_name: str, module_map: dict) -> list:
    result = list()
    for name, path in module_map.items():
        result.append(_import_one(module_name=module_name, module_map=module_map, name=name))
    return result


def _import_all_generously(module_name: str, module_map: dict, required: set) -> list:
    result = _import_all(module_name=module_name, module_map={k: v for k, v in module_map.items() if k in required})
    for k in (module_map.keys() - required):
        try:
            result.append(_import_one(module_name=__name__, module_map=module_map, name=k))
        except Exception as e:
            print(e)
            continue
    return result


def _set_submodule(module) -> None:
    classes = _import_all_generously(module_name=module.__name__,
                                     module_map=module.__lazy_imports__,
                                     required=module.__required__)
    for c in classes:
        setattr(module, c.__name__, c)


if sys.version_info < (3, 7):
    _import_all_generously(module_name=__name__, module_map=__lazy_imports__, required=__required__)
    _set_submodule(module=interfaces)
    _set_submodule(module=storages)
