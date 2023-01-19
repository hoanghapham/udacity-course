import enum

class LoadMode(enum.Enum):
    APPEND_ONLY = "APPEND_ONLY"
    DELETE_INSERT = "DELETE_INSERT"