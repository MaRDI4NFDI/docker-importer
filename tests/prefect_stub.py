"""Shared Prefect test stub.

Always replaces any installed Prefect modules with lightweight stand-ins so
tests never import the real library. Safe to call multiple times.
"""

from types import ModuleType, SimpleNamespace
import sys


def install_prefect_stub(force: bool = True) -> None:
    if not force and "prefect" in sys.modules:
        return

    pref = ModuleType("prefect")
    states = ModuleType("prefect.states")
    artifacts = ModuleType("prefect.artifacts")
    blocks = ModuleType("prefect.blocks")
    blocks_system = ModuleType("prefect.blocks.system")
    context = ModuleType("prefect.context")
    deployments = ModuleType("prefect.deployments")
    logging_mod = ModuleType("prefect.logging")
    exceptions = ModuleType("prefect.exceptions")

    pref.__path__ = []
    states.__path__ = []
    artifacts.__path__ = []
    blocks.__path__ = []
    blocks_system.__path__ = []
    context.__path__ = []
    deployments.__path__ = []
    logging_mod.__path__ = []

    def task(*_args, **_kwargs):
        def decorator(fn):
            return fn
        return decorator

    def flow(*_args, **_kwargs):
        def decorator(fn):
            return fn
        return decorator

    class State:
        pass

    class Failed(State):
        def __init__(self, message=None, data=None):
            self.message = message
            self.data = data

    class Artifact:
        def __init__(self, type=None, key=None, description=None, data=None):
            self.type = type
            self.key = key
            self.description = description
            self.data = data
            self.id = "artifact-id"

        def create(self):
            return self

    class Secret:
        @classmethod
        def load(cls, _name):
            return SimpleNamespace(get=lambda: "secret")

    def get_run_logger():
        import logging
        logger = logging.getLogger("prefect-stub")
        return logger

    def get_run_context():
        return SimpleNamespace(flow_run=SimpleNamespace(id="flow-run-id"))

    class FlowRunContext:
        def __init__(self, flow_run=None):
            self.flow_run = flow_run or SimpleNamespace(id="flow-run-id")

    class TaskRunContext:
        pass

    def run_deployment(*_args, **_kwargs):
        return SimpleNamespace(deployment_id="deployment-id", id="run-id", flow_id="flow-id")

    class MissingContextError(Exception):
        pass

    pref.task = task
    pref.flow = flow
    pref.get_run_logger = get_run_logger
    pref.State = State

    states.Failed = Failed
    artifacts.Artifact = Artifact
    blocks.system = blocks_system
    blocks_system.Secret = Secret
    context.get_run_context = get_run_context
    context.FlowRunContext = FlowRunContext
    context.TaskRunContext = TaskRunContext
    deployments.run_deployment = run_deployment
    logging_mod.get_run_logger = get_run_logger
    exceptions.MissingContextError = MissingContextError

    sys.modules["prefect"] = pref
    sys.modules["prefect.states"] = states
    sys.modules["prefect.artifacts"] = artifacts
    sys.modules["prefect.blocks"] = blocks
    sys.modules["prefect.blocks.system"] = blocks_system
    sys.modules["prefect.context"] = context
    sys.modules["prefect.deployments"] = deployments
    sys.modules["prefect.logging"] = logging_mod
    sys.modules["prefect.exceptions"] = exceptions
