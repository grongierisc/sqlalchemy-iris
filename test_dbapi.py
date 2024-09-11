import iris
import pytest
from intersystems_iris import connect as iris_connect
from sqlalchemy import create_engine, text

class TestSimpleTable():

    # before each test, create a new table
    def setup_method(self):
        self.table = SimpleTable()

    # after each test, delete the table
    def teardown_method(self):
        self.table.delete()

    def test_insert(self):
        self.table.cursor.execute('''
            INSERT INTO simple_table (id, var1, int1, date1)
            VALUES (?, ?, ?, ?)
        ''', (1,'test', 1, '2020-01-01'))
        self.table.conn.commit()

    def test_select(self):
        self.table.cursor.execute('''
            SELECT * FROM simple_table
        ''')
        rows = self.table.cursor.fetchall()
        assert len(rows) == 0

    def test_null(self):
        self.table.cursor.execute('''
            INSERT INTO simple_table (id, var1, int1, date1)
            VALUES (?, ?, ?, ?)
        ''', (1, None, None, None))
        self.table.conn.commit()
        self.table.cursor.execute('''
            SELECT * FROM simple_table
        ''')
        rows = self.table.cursor.fetchall()
        assert len(rows) == 1
        assert rows[0][1] == None
        assert rows[0][2] == None
        assert rows[0][3] == None

    def test_roll_back(self):
        with self.table.cursor as cursor:
            # set autocommit to false
            self.table.conn.setAutoCommit(False)
            cursor.execute('''
                INSERT INTO simple_table (id, var1, int1, date1)
                VALUES (?, ?, ?, ?)
            ''', (1, 'test', 1, '2020-01-01'))
            self.table.conn.rollback()
            cursor.execute('''
                SELECT * FROM simple_table
            ''')
            rows = cursor.fetchall()
            assert len(rows) == 0

    def test_with_thread(self):
        # create a new thread
        import threading
        thread = threading.Thread(target=self.test_insert)
        thread.start()
        thread.join()
        self.table.cursor.execute('''
            SELECT * FROM simple_table
        ''')

    def test_insert_to_non_existent_table(self):
        with pytest.raises(Exception):
            self.table.cursor.execute('''
                INSERT INTO non_existent_table (id, var1, int1, date1)
                VALUES (?, ?, ?, ?)
            ''', (1, 'test', 1, '2020-01-01'))

    @pytest.mark.skip(reason="fetchone is not working properly after cursor is closed")
    def test_null_fetchone(self):
        self.table.cursor.execute('''
            INSERT INTO simple_table (id, var1, int1, date1)
            VALUES (?, ?, ?, ?)
        ''', (1, None, None, None))
        self.table.conn.commit()
        self.table.cursor.execute('''
            SELECT * FROM simple_table
        ''')
        row = self.table.cursor.fetchone()
        # close the cursor
        self.table.cursor.close()
        assert row[1] == None # segfault here
        assert row[2] == None
        assert row[3] == None

        # fetch again
        self.table.cursor.fetchall()

    def test_fetchone_after_cursor_close(self):
        self.table.cursor.execute('''
            SELECT 1
        ''')
        row = self.table.cursor.fetchone()
        self.table.cursor.close()
        assert row[0][0] == 1


class TestBooleanTable():

        # before each test, create a new table
        def setup_method(self):
            self.table = BooleanTable()

        # after each test, delete the table
        def teardown_method(self):
            self.table.delete()

        def test_insert(self):
            self.table.cursor.execute('''
                INSERT INTO boolean_table (id, bool1)
                VALUES (?, ?)
            ''', (1, True))
            self.table.conn.commit()

        def test_select(self):
            self.table.cursor.execute('''
                SELECT * FROM boolean_table
            ''')
            rows = self.table.cursor.fetchall()
            assert len(rows) == 0

        def test_null(self):
            self.table.cursor.execute('''
                INSERT INTO boolean_table (id, bool1)
                VALUES (?, ?)
            ''', (1, None))
            self.table.conn.commit()
            self.table.cursor.execute('''
                SELECT * FROM boolean_table
            ''')
            rows = self.table.cursor.fetchall()
            assert len(rows) == 1
            assert rows[0][1] == None

        def test_round_trip(self):
            self.table.cursor.execute('''
                INSERT INTO boolean_table (id, bool1)
                VALUES (?, ?)
            ''', (1, True))
            self.table.cursor.execute('''
                SELECT * FROM boolean_table
            ''')
            rows = self.table.cursor.fetchone()
            assert rows[1] == True

        def test_close_cursor_twice(self):
            self.table.cursor.close()
            self.table.cursor.close()

        def test_cursor_with_params(self):
            cursor = self.table.conn.cursor()
            cursor.execute('''
                INSERT INTO boolean_table (id, bool1)
                VALUES (?, ?)
            ''', (1, True))

        def test_close_connection_recovers(self):
            self.table.conn.close()
            self.table.conn=iris.connect('localhost', 51773, 'USER', 'SuperUser', 'SYS')
            self.table.cursor=self.table.conn.cursor()
            self.table.cursor.execute('''
                SELECT * FROM boolean_table
            ''')
            rows = self.table.cursor.fetchall()
            assert len(rows) == 0

class TestBlobTable():

            # before each test, create a new table
            def setup_method(self):
                self.table = BlobTable()

            # after each test, delete the table
            def teardown_method(self):
                self.table.delete()

            def test_insert(self):
                self.table.cursor.execute('''
                    INSERT INTO blob_table (id, blob1, blob2)
                    VALUES (?, ?, ?)
                ''', (1, 'test', b'test'))
                self.table.conn.commit()

            def test_select(self):
                self.table.cursor.execute('''
                    SELECT * FROM blob_table
                ''')
                rows = self.table.cursor.fetchall()
                assert len(rows) == 0

            def test_null(self):
                self.table.cursor.execute('''
                    INSERT INTO blob_table (id, blob1, blob2)
                    VALUES (?, ?, ?)
                ''', (1, None, None))
                self.table.conn.commit()
                self.table.cursor.execute('''
                    SELECT * FROM blob_table
                ''')
                rows = self.table.cursor.fetchall()
                assert len(rows) == 1
                assert rows[0][1] == None
                assert rows[0][2] == None

            def test_round_trip(self):
                self.table.cursor.execute('''
                    INSERT INTO blob_table (id, blob1, blob2)
                    VALUES (?, ?, ?)
                ''', (1, 'test', b'test'))
                self.table.cursor.execute('''
                    SELECT * FROM blob_table
                ''')
                rows = self.table.cursor.fetchone()
                assert rows[1] == 'test'
                assert rows[2] == b'test'

            def test_long_blob(self):
                size = 100000000 # 10MB
                self.table.cursor.execute('''
                    INSERT INTO blob_table (id, blob1, blob2)
                    VALUES (?, ?, ?)
                ''', (1, 'a'*size, b'a'*size))
                self.table.conn.commit()
                self.table.cursor.execute('''
                    SELECT * FROM blob_table
                ''')
                rows = self.table.cursor.fetchall()
                assert len(rows) == 1
                assert rows[0][1] == 'a'*size
                assert rows[0][2] == b'a'*size


class BooleanTable():

        def __init__(self):
            self.conn = iris.connect('localhost', 51773, 'USER', 'SuperUser', 'SYS')
            self.cursor = self.conn.cursor()
            try:
                self.cursor.execute('DROP TABLE boolean_table')
            except:
                pass
            self.create()

        def create(self):
            self.cursor.execute('''
                CREATE TABLE boolean_table (
                    id INTEGER PRIMARY KEY,
                    bool1 BIT
                )
            ''')

        def delete(self):
            if self.cursor.isClosed():
                self.cursor = self.conn.cursor()
            self.cursor.execute('DROP TABLE boolean_table')

class SimpleTable():

    def __init__(self):
        self.conn = iris.connect('localhost', 51773, 'USER', 'SuperUser', 'SYS')
        self.cursor = self.conn.cursor()
        try:
            self.cursor.execute('DROP TABLE simple_table')
        except:
            pass
        self.create()

    def create(self):
        self.cursor.execute('''
            CREATE TABLE simple_table (
                id INTEGER PRIMARY KEY,
                var1 VARCHAR(255),
                int1 INTEGER,
                date1 DATE
            )
        ''')

    def delete(self):
        if self.cursor.isClosed():
            self.cursor = self.conn.cursor()
        self.cursor.execute('DROP TABLE simple_table')

class BlobTable():

        def __init__(self):
            self.conn = iris.connect('localhost', 51773, 'USER', 'SuperUser', 'SYS')
            self.cursor = self.conn.cursor()
            try:
                self.cursor.execute('DROP TABLE blob_table')
            except:
                pass
            self.create()

        def create(self):
            self.cursor.execute('''
                CREATE TABLE blob_table (
                    id INTEGER PRIMARY KEY,
                    blob1 TEXT,
                    blob2 LONGVARBINARY
                )
            ''')

        def delete(self):
            if self.cursor.isClosed():
                self.cursor = self.conn.cursor()
            self.cursor.execute('DROP TABLE blob_table')

class TestSQLAlchemySimple():

    dburi = 'iris+intersystems://SuperUser:SYS@localhost:51773/USER'

    def test_with_connection(self):

        engine = create_engine(self.dburi)
        with engine.connect() as conn:
            result = conn.execute(text('SELECT 1'))
            assert result.fetchone()[0] == 1

        with engine.connect() as conn:
            result = conn.execute(text('SELECT 1'))
            rows = result.fetchall()
            assert len(rows) == 1

    def test_with_positional_params(self):

        engine = create_engine(self.dburi)
        with engine.connect() as conn:
            result = conn.execute(text('SELECT :value, :value2'), [{'value': 1, 'value2': 2}])
            row = result.fetchone()
            assert row[0] == 1
            assert row[1] == 2

    def test_run_query_on_closed_connection(self):

        engine = create_engine(self.dburi)
        conn = engine.connect()
        conn.close()
        with pytest.raises(Exception):
            result = conn.execute(text('SELECT 1'))
            row = result.fetchone()
            assert row[0] == 1

