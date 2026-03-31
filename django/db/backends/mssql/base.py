"""
Microsoft SQL Server database backend for Django.

Requires pyodbc: pip install pyodbc
Driver: ODBC Driver 17 for SQL Server (or 18)
"""

from django.core.exceptions import ImproperlyConfigured
from django.db.backends.base.base import BaseDatabaseWrapper

try:
    import pyodbc as Database
except ImportError:
    raise ImproperlyConfigured(
        "Error loading pyodbc module. Install it with: pip install pyodbc"
    )

from .client import DatabaseClient  # NOQA
from .creation import DatabaseCreation  # NOQA
from .features import DatabaseFeatures  # NOQA
from .introspection import DatabaseIntrospection  # NOQA
from .operations import DatabaseOperations  # NOQA
from .schema import DatabaseSchemaEditor  # NOQA


class DatabaseWrapper(BaseDatabaseWrapper):
    vendor = "microsoft"
    display_name = "SQL Server"

    data_types = {
        "AutoField": "int IDENTITY (1, 1)",
        "BigAutoField": "bigint IDENTITY (1, 1)",
        "SmallAutoField": "smallint IDENTITY (1, 1)",
        "BinaryField": "varbinary(max)",
        "BooleanField": "bit",
        "CharField": "nvarchar(%(max_length)s)",
        "DateField": "date",
        "DateTimeField": "datetime2",
        "DecimalField": "numeric(%(max_digits)s, %(decimal_places)s)",
        "DurationField": "bigint",
        "FileField": "nvarchar(%(max_length)s)",
        "FilePathField": "nvarchar(%(max_length)s)",
        "FloatField": "float",
        "IntegerField": "int",
        "BigIntegerField": "bigint",
        "IPAddressField": "nvarchar(15)",
        "GenericIPAddressField": "nvarchar(39)",
        "JSONField": "nvarchar(max)",
        "PositiveBigIntegerField": "bigint",
        "PositiveIntegerField": "int",
        "PositiveSmallIntegerField": "smallint",
        "SlugField": "nvarchar(%(max_length)s)",
        "SmallIntegerField": "smallint",
        "TextField": "nvarchar(max)",
        "TimeField": "time",
        "UUIDField": "uniqueidentifier",
    }

    data_type_check_constraints = {
        "PositiveBigIntegerField": "[%(column)s] >= 0",
        "PositiveIntegerField": "[%(column)s] >= 0",
        "PositiveSmallIntegerField": "[%(column)s] >= 0",
    }

    operators = {
        "exact": "= %s",
        "iexact": "LIKE %s",
        "contains": "LIKE %s",
        "icontains": "LIKE %s",
        "gt": "> %s",
        "gte": ">= %s",
        "lt": "< %s",
        "lte": "<= %s",
        "startswith": "LIKE %s",
        "endswith": "LIKE %s",
        "istartswith": "LIKE %s",
        "iendswith": "LIKE %s",
    }

    pattern_esc = "REPLACE(REPLACE(REPLACE({}, N'\\', N'\\\\'), N'%', N'\\%'), N'_', N'\\_')"
    pattern_ops = {
        "contains": "LIKE N'%' + {} + N'%'",
        "icontains": "LIKE N'%' + {} + N'%'",
        "startswith": "LIKE {} + N'%'",
        "istartswith": "LIKE {} + N'%'",
        "endswith": "LIKE N'%' + {}",
        "iendswith": "LIKE N'%' + {}",
    }

    Database = Database
    SchemaEditorClass = DatabaseSchemaEditor
    client_class = DatabaseClient
    creation_class = DatabaseCreation
    features_class = DatabaseFeatures
    introspection_class = DatabaseIntrospection
    ops_class = DatabaseOperations

    def get_connection_params(self):
        settings_dict = self.settings_dict
        if not settings_dict["NAME"]:
            raise ImproperlyConfigured(
                "settings.DATABASES is improperly configured. "
                "Please supply the NAME value."
            )

        options = settings_dict.get("OPTIONS", {})
        driver = options.get("driver", "ODBC Driver 17 for SQL Server")

        conn_params = {
            "driver": driver,
            "server": settings_dict.get("HOST", "127.0.0.1"),
            "database": settings_dict["NAME"],
            "user": settings_dict.get("USER", ""),
            "password": settings_dict.get("PASSWORD", ""),
        }

        port = settings_dict.get("PORT")
        if port:
            conn_params["server"] = f"{conn_params['server']},{port}"

        # Extra ODBC options
        conn_params.update(
            {k: v for k, v in options.items() if k not in ("driver",)}
        )
        return conn_params

    def get_new_connection(self, conn_params):
        driver = conn_params.pop("driver")
        server = conn_params.pop("server")
        database = conn_params.pop("database")
        user = conn_params.pop("user")
        password = conn_params.pop("password")

        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
        )

        # Append any remaining options
        for key, value in conn_params.items():
            conn_str += f"{key}={value};"

        conn = Database.connect(conn_str)
        # Use MARS (Multiple Active Result Sets) if available
        return conn

    def init_connection_state(self):
        super().init_connection_state()
        # Set ANSI defaults required for Django compatibility
        with self.cursor() as cursor:
            cursor.execute("SET ANSI_NULLS ON")
            cursor.execute("SET ANSI_WARNINGS ON")
            cursor.execute("SET ANSI_PADDING ON")
            cursor.execute("SET QUOTED_IDENTIFIER ON")
            cursor.execute("SET CONCAT_NULL_YIELDS_NULL ON")

    def create_cursor(self, name=None):
        cursor = self.connection.cursor()
        return CursorWrapper(cursor)

    def _set_autocommit(self, autocommit):
        with self.wrap_database_errors:
            self.connection.autocommit = autocommit

    def is_usable(self):
        if self.connection is None:
            return False
        try:
            self.connection.cursor().execute("SELECT 1")
        except Database.Error:
            return False
        return True

    def _savepoint_allowed(self):
        return self.in_atomic_block

    def _savepoint(self, sid):
        with self.cursor() as cursor:
            cursor.execute(f"SAVE TRANSACTION {sid}")

    def _savepoint_rollback(self, sid):
        with self.cursor() as cursor:
            cursor.execute(f"ROLLBACK TRANSACTION {sid}")

    def _savepoint_commit(self, sid):
        # SQL Server doesn't support releasing savepoints; just a no-op
        pass

    def check_constraints(self, table_names=None):
        with self.cursor() as cursor:
            if table_names:
                for table in table_names:
                    cursor.execute(
                        "DBCC CHECKCONSTRAINTS([%s])" % table
                    )
            else:
                cursor.execute("DBCC CHECKCONSTRAINTS")


class CursorWrapper:
    """Wrapper around pyodbc cursor to handle MSSQL quirks."""

    def __init__(self, cursor):
        self.cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def __iter__(self):
        return iter(self.cursor)

    def __getattr__(self, attr):
        return getattr(self.cursor, attr)

    def callproc(self, procname, params=None):
        if params is None:
            params = []
        placeholders = ", ".join(["?"] * len(params))
        sql = f"{{CALL {procname}({placeholders})}}"
        self.cursor.execute(sql, params)
        return params

    def execute(self, sql, params=None):
        # pyodbc uses ? as placeholder, Django uses %s
        sql = self._format_sql(sql)
        if params is None:
            self.cursor.execute(sql)
        else:
            self.cursor.execute(sql, params)

    def executemany(self, sql, param_list):
        sql = self._format_sql(sql)
        self.cursor.executemany(sql, param_list)

    def _format_sql(self, sql):
        # Replace Django's %s placeholders with pyodbc's ?
        # Be careful not to replace %% (escaped percent)
        result = []
        i = 0
        while i < len(sql):
            if sql[i] == '%' and i + 1 < len(sql):
                if sql[i + 1] == 's':
                    result.append('?')
                    i += 2
                elif sql[i + 1] == '%':
                    result.append('%')
                    i += 2
                else:
                    result.append(sql[i])
                    i += 1
            else:
                result.append(sql[i])
                i += 1
        return ''.join(result)

    @property
    def rowcount(self):
        return self.cursor.rowcount

    @property
    def description(self):
        return self.cursor.description

    @property
    def lastrowid(self):
        return self.cursor.rowcount

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchmany(self, size=None):
        if size is None:
            return self.cursor.fetchmany()
        return self.cursor.fetchmany(size)

    def fetchall(self):
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()

    def setinputsizes(self, *args):
        pass

    def setoutputsize(self, *args):
        pass
