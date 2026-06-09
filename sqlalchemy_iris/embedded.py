from sqlalchemy import exc
from sqlalchemy import util

from .base import IRISDialect


def _parse_version_number(server_version):
    server_version = str(server_version).split(" ")[0].split(".")
    return tuple([int("".join(filter(str.isdigit, v)) or 0) for v in server_version])


class IRISDialect_emb(IRISDialect):
    driver = "emb"

    embedded = True

    supports_statement_cache = True

    insert_returning = False
    insert_executemany_returning = False
    insert_executemany_returning_sort_by_parameter_order = False

    _isolation_lookup = set(
        [
            "READ UNCOMMITTED",
            "READ COMMITTED",
            "REPEATABLE READ",
            "SERIALIZABLE",
        ]
    )

    def _get_option(self, connection, option):
        import iris

        return iris.cls("%SYSTEM.SQL.Util").GetOption(option)

    def _set_option(self, connection, option, value):
        import iris

        return iris.cls("%SYSTEM.SQL.Util").SetOption(option, value)

    @classmethod
    def import_dbapi(cls):
        import iris

        return iris.dbapi

    def create_connect_args(self, url):
        if url.host or url.port or url.username or url.password:
            raise exc.ArgumentError(
                "iris+emb:// URLs are local-only; use iris:// or iris+intersystems:// "
                "for host, port, username, or password connections"
            )

        supported_query_args = {"path"}
        unsupported_query_args = set(url.query).difference(supported_query_args)
        if unsupported_query_args:
            raise exc.ArgumentError(
                "Unsupported iris+emb:// query argument(s): "
                + ", ".join(sorted(unsupported_query_args))
            )

        opts = {
            "mode": "embedded",
            "namespace": url.database if url.database else "USER",
        }
        path = url.query.get("path")
        if path is not None:
            if isinstance(path, tuple):
                path = path[-1]
            opts["path"] = path

        return ([], opts)

    def _get_server_version_info(self, connection):
        import iris

        version_api = getattr(getattr(iris, "system", None), "Version", None)
        get_number = getattr(version_api, "GetNumber", None)
        if callable(get_number):
            return _parse_version_number(get_number())

        return _parse_version_number(iris.cls("%SYSTEM.Version").GetNumber())

    def on_connect(self):
        def on_connect(conn):
            try:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "select vector_cosine(to_vector('1'), to_vector('1'))"
                    )
                    cursor.execute("select to_vector('1')")
                    cursor.fetchone()
                self.supports_vectors = True
            except Exception:
                self.supports_vectors = False

            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT TOP 1 Name FROM %Dictionary.PropertyDefinition")
                    cursor.fetchone()
                self._dictionary_access = True
            except Exception:
                self._dictionary_access = False

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

    def get_isolation_level(self, connection):
        if getattr(connection, "autocommit", False):
            return "AUTOCOMMIT"

        isolation_level = getattr(connection, "isolation_level", None)
        if isolation_level:
            return isolation_level.upper()

        return "READ COMMITTED"

    def set_isolation_level(self, connection, level_str):
        if level_str == "AUTOCOMMIT":
            connection.autocommit = True
        else:
            connection.autocommit = False
            connection.isolation_level = level_str

    def do_execute(self, cursor, query, params, context=None):
        if query.endswith(";"):
            query = query[:-1]
        self._debug(query, params)
        cursor.execute(query, params)

    def do_executemany(self, cursor, query, params, context=None):
        if query.endswith(";"):
            query = query[:-1]
        self._debug(query, params, True)
        cursor.executemany(query, params)


dialect = IRISDialect_emb
