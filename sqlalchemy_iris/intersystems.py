import pkg_resources
from .base import IRISDialect
from sqlalchemy import text, util
from . import intersystems_dbapi as dbapi
from iris import connect

class IRISDialect_intersystems(IRISDialect):
    driver = "intersystems"

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
        return connect(host, port, namespace, username, password)

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
            row = cursor.fetchall()
            if row:
                return row[0][0]
        return None

    def do_rollback(self, connection):
        connection.connection.rollback()

    def do_commit(self, connection):
        connection.connection.commit()

    def do_close(self, connection):
        return connection.connection.close()

    def do_execute(self, cursor, query, params, context=None):
        return super().do_execute(cursor, query, params, context)

dialect = IRISDialect_intersystems
