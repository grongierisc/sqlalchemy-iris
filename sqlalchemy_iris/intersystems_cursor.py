from typing import Any
from sqlalchemy import CursorResult
from sqlalchemy.engine.cursor import CursorFetchStrategy
from sqlalchemy.engine.interfaces import DBAPICursor


class InterSystemsCursorFetchStrategy(CursorFetchStrategy):

    def fetchone(
        self,
        result: CursorResult[Any],
        dbapi_cursor: DBAPICursor,
        hard_close: bool = False,
    ) -> Any:
        try:
            row = dbapi_cursor.fetchone()
            if row is None:
                result._soft_close(hard=hard_close)
            else:
                return tuple(row)
        except BaseException as e:
            self.handle_exception(result, dbapi_cursor, e)

    def fetchall(
        self,
        result: CursorResult[Any],
        dbapi_cursor: DBAPICursor,
    ) -> Any:
        try:
            rows = dbapi_cursor.fetchall()
            # avoid segfaults for cursors closed before fetchall
            for row in rows:
                pass
            #result._soft_close()
            return rows
        except BaseException as e:
            self.handle_exception(result, dbapi_cursor, e)
