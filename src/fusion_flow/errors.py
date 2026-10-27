import logging

logger = logging.getLogger("fusion_flow")


class FlowError(Exception):
    pass


class QueueFull(FlowError):
    pass


class IllegalTransition(FlowError):
    pass


class UnknownKind(FlowError):
    pass


class ConnectorError(FlowError):
    pass


class PayloadTooLarge(FlowError):
    pass


# Retryable failure: re-running later may succeed. Handlers raise this to trigger backoff + retry.
class TransientError(FlowError):
    pass


# Deterministic failure: input itself is broken. No retry, job goes to a terminal failed state.
class PermanentError(FlowError):
    pass


class WorkerLockHeld(FlowError):
    pass
