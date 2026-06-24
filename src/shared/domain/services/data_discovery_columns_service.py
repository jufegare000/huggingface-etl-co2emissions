from mypy.semanal_shared import Protocol


class DataDiscoveryColumnsService(Protocol):
    def get_columns(self):
        ...