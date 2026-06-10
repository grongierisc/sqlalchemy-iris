try:
    import iris

    class Cursor(iris.irissdk.dbapiCursor):
        pass

    class DataRow(iris.irissdk.dbapiDataRow):
        pass

except (AttributeError, ImportError, TypeError):
    iris = None


def connect(*args, **kwargs):
    _sync_exception_classes()
    try:
        return iris.connect(*args, **kwargs)
    finally:
        _sync_exception_classes()


def createIRIS(*args, **kwargs):
    return iris.createIRIS(*args, **kwargs)


# globals
apilevel = "2.0"
threadsafety = 0
paramstyle = "qmark"

Binary = bytes
STRING = str
BINARY = bytes
NUMBER = float
ROWID = str

class Error(Exception):
    pass


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class InternalError(DatabaseError):
    pass


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    pass


_EXCEPTION_NAMES = (
    "Error",
    "Warning",
    "InterfaceError",
    "DatabaseError",
    "InternalError",
    "OperationalError",
    "ProgrammingError",
    "IntegrityError",
    "DataError",
    "NotSupportedError",
)


def _sync_exception_classes():
    if iris is None or not hasattr(iris, "dbapi"):
        return

    for name in _EXCEPTION_NAMES:
        cls = getattr(iris.dbapi, name, None)
        if cls is not None:
            globals()[name] = cls


_sync_exception_classes()
