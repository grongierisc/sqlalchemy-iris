import pkg_resources
from .base import IRISDialect
from sqlalchemy import text, util
from .base import IRISExecutionContext
from . import intersystems_dbapi as dbapi
from .intersystems_dbapi import connect
from .intersystems_cursor import InterSystemsCursorFetchStrategy

class InterSystemsExecutionContext(IRISExecutionContext):
    cursor_fetch_strategy = InterSystemsCursorFetchStrategy()

class IRISDialect_intersystems(IRISDialect):
    driver = "intersystems"

    execution_ctx_cls = InterSystemsExecutionContext

    supports_statement_cache = True

    sqlcode = None

    @classmethod
    def import_dbapi(cls):
        return dbapi

    def connect(self, *cargs, **kwarg):
        host = kwarg.get('hostname', 'localhost')
        port = kwarg.get('port', 1972)
        namespace = kwarg.get('namespace', 'USER')
        username = kwarg.get('username', '_SYSTEM')
        password = kwarg.get('password', 'SYS')
        timeout = kwarg.get('timeout', 10)
        sharedmemory = kwarg.get('sharedmemory', False)
        logfile = kwarg.get('logfile', '')
        sslconfig = kwarg.get('sslconfig', False)
        autoCommit = kwarg.get('autoCommit', False)
        isolationLevel = kwarg.get('isolationLevel', 0)
        return connect(host, port, namespace, username, password, timeout, sharedmemory, logfile, sslconfig, autoCommit, isolationLevel)

    def on_connect(self):

        def on_connect(conn):

            try:
                with conn.cursor() as cursor:
                    cursor.execute(text("SELECT TO_VECTOR('1,2,3', INT, 3)"))
                self.supports_vectors = True
            except:  # noqa
                self.supports_vectors = False
            if self.supports_vectors:
                with conn.cursor() as cursor:
                    # Distance or similarity
                    cursor.execute(
                        "select vector_cosine(to_vector('1'), to_vector('1'))"
                    )
                    self.vector_cosine_similarity = cursor.fetchone()[0] == 0

            self._dictionary_access = False
            with conn.cursor() as cursor:
                res = cursor.execute("%CHECKPRIV SELECT ON %Dictionary.PropertyDefinition")
                self._dictionary_access = res == 0

            if not self._dictionary_access:
                util.warn(
                    """
There are no access to %Dictionary, may be required for some advanced features,
 such as Calculated fields, and include columns in indexes
                """.replace(
                        "\n", ""
                    )
                )

        return on_connect

    def create_connect_args(self, url):
        opts = {}

        opts["application_name"] = "sqlalchemy"
        opts["host"] = url.host
        opts["port"] = int(url.port) if url.port else 1972
        opts["namespace"] = url.database if url.database else "USER"
        opts["username"] = url.username if url.username else ""
        opts["password"] = url.password if url.password else ""

        opts["autoCommit"] = False

        if opts["host"] and "@" in opts["host"]:
            _h = opts["host"].split("@")
            opts["password"] += "@" + _h[0 : len(_h) - 1].join("@")
            opts["host"] = _h[len(_h) - 1]

        return ([], opts)

    def _get_server_version_info(self, connection):
        # get the wheel version from iris module
        try:
            return tuple(map(int, pkg_resources.get_distribution("intersystems_irispython").version.split(".")))
        except:  # noqa
            return None

    def _get_option(self, connection, option):
        with connection.cursor() as cursor:
            cursor.execute("SELECT %SYSTEM_SQL.Util_GetOption(?)", (option,))
            row = cursor.fetchone()
            if row:
                return row[0]
        return None

    def set_isolation_level(self, connection, level_str):
        if level_str == "AUTOCOMMIT":
            connection.autocommit = True
        else:
            connection.autocommit = False
            if level_str not in ["READ COMMITTED", "READ VERIFIED"]:
                level_str = "READ UNCOMMITTED"
            with connection.cursor() as cursor:
                cursor.execute("SET TRANSACTION ISOLATION LEVEL " + level_str)

dialect = IRISDialect_intersystems
