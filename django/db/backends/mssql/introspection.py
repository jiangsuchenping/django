from collections import namedtuple

from django.db.backends.base.introspection import BaseDatabaseIntrospection
from django.db.backends.base.introspection import FieldInfo as BaseFieldInfo
from django.db.backends.base.introspection import TableInfo
from django.db.models import DO_NOTHING

FieldInfo = namedtuple("FieldInfo", [*BaseFieldInfo._fields, "is_autofield", "comment"])


class DatabaseIntrospection(BaseDatabaseIntrospection):
    # Maps SQL Server type names to Django field types
    data_types_reverse = {
        "bigint": "BigIntegerField",
        "binary": "BinaryField",
        "bit": "BooleanField",
        "char": "CharField",
        "date": "DateField",
        "datetime": "DateTimeField",
        "datetime2": "DateTimeField",
        "datetimeoffset": "DateTimeField",
        "decimal": "DecimalField",
        "float": "FloatField",
        "image": "BinaryField",
        "int": "IntegerField",
        "money": "DecimalField",
        "nchar": "CharField",
        "ntext": "TextField",
        "numeric": "DecimalField",
        "nvarchar": "CharField",
        "real": "FloatField",
        "smalldatetime": "DateTimeField",
        "smallint": "SmallIntegerField",
        "smallmoney": "DecimalField",
        "text": "TextField",
        "time": "TimeField",
        "tinyint": "SmallIntegerField",
        "uniqueidentifier": "UUIDField",
        "varbinary": "BinaryField",
        "varchar": "CharField",
        "xml": "TextField",
    }

    def get_field_type(self, data_type, description):
        field_type = super().get_field_type(data_type, description)
        if description.is_autofield:
            if field_type == "IntegerField":
                return "AutoField"
            elif field_type == "BigIntegerField":
                return "BigAutoField"
            elif field_type == "SmallIntegerField":
                return "SmallAutoField"
        # nvarchar(max) -> TextField
        if data_type == "nvarchar" and description.internal_size == -1:
            return "TextField"
        if data_type == "varchar" and description.internal_size == -1:
            return "TextField"
        return field_type

    def get_table_list(self, cursor):
        """Return a list of table and view names in the current database."""
        cursor.execute("""
            SELECT
                t.name,
                CASE WHEN t.type = 'V' THEN 'v' ELSE 't' END
            FROM sys.objects t
            WHERE t.type IN ('U', 'V')
            ORDER BY t.name
        """)
        return [TableInfo(*row) for row in cursor.fetchall()]

    def get_table_description(self, cursor, table_name):
        """Return a description of the table columns."""
        cursor.execute(
            """
            SELECT
                c.name AS column_name,
                tp.name AS data_type,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity,
                OBJECT_DEFINITION(c.default_object_id) AS column_default,
                ep.value AS column_comment
            FROM sys.columns c
            JOIN sys.types tp ON c.user_type_id = tp.user_type_id
            JOIN sys.objects o ON c.object_id = o.object_id
            LEFT JOIN sys.extended_properties ep
                ON ep.major_id = c.object_id
                AND ep.minor_id = c.column_id
                AND ep.name = 'MS_Description'
            WHERE o.name = %s
            ORDER BY c.column_id
            """,
            [table_name],
        )
        rows = cursor.fetchall()

        # Also run a SELECT to get cursor.description for type codes
        cursor.execute(
            "SELECT TOP 1 * FROM %s" % self.connection.ops.quote_name(table_name)
        )
        desc = cursor.description or []
        desc_map = {d[0]: d for d in desc}

        result = []
        for row in rows:
            col_name, data_type, max_length, precision, scale, is_nullable, is_identity, default, comment = row
            # internal_size: -1 means MAX
            internal_size = max_length if max_length != -1 else None
            d = desc_map.get(col_name)
            result.append(
                FieldInfo(
                    col_name,
                    data_type,
                    None,  # display_size
                    internal_size,
                    precision,
                    scale,
                    bool(is_nullable),
                    default,
                    bool(is_identity),
                    comment,
                )
            )
        return result

    def get_sequences(self, cursor, table_name, table_fields=()):
        # SQL Server uses IDENTITY columns, not sequences
        cursor.execute(
            """
            SELECT c.name
            FROM sys.columns c
            JOIN sys.objects o ON c.object_id = o.object_id
            WHERE o.name = %s AND c.is_identity = 1
            """,
            [table_name],
        )
        row = cursor.fetchone()
        if row:
            return [{"name": None, "table": table_name, "column": row[0]}]
        return []

    def get_relations(self, cursor, table_name):
        """
        Return a dict of {field_name: (field_name_other_table, other_table, on_delete)}.
        """
        cursor.execute(
            """
            SELECT
                kcu1.COLUMN_NAME,
                kcu2.TABLE_NAME,
                kcu2.COLUMN_NAME
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu1
                ON kcu1.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2
                ON kcu2.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
            WHERE kcu1.TABLE_NAME = %s
            """,
            [table_name],
        )
        return {
            row[0]: (row[2], row[1], DO_NOTHING)
            for row in cursor.fetchall()
        }

    def get_constraints(self, cursor, table_name):
        """Retrieve constraints, keys, and indexes for a table."""
        constraints = {}

        # Primary keys and unique constraints
        cursor.execute(
            """
            SELECT
                kc.CONSTRAINT_NAME,
                kc.COLUMN_NAME,
                tc.CONSTRAINT_TYPE
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE kc
            JOIN INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                ON kc.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                AND kc.TABLE_NAME = tc.TABLE_NAME
            WHERE kc.TABLE_NAME = %s
            ORDER BY kc.CONSTRAINT_NAME, kc.ORDINAL_POSITION
            """,
            [table_name],
        )
        for constraint_name, column_name, constraint_type in cursor.fetchall():
            if constraint_name not in constraints:
                constraints[constraint_name] = {
                    "columns": [],
                    "primary_key": constraint_type == "PRIMARY KEY",
                    "unique": constraint_type in ("PRIMARY KEY", "UNIQUE"),
                    "foreign_key": None,
                    "check": constraint_type == "CHECK",
                    "index": False,
                    "definition": None,
                    "options": None,
                }
            constraints[constraint_name]["columns"].append(column_name)

        # Foreign keys
        cursor.execute(
            """
            SELECT
                rc.CONSTRAINT_NAME,
                kcu1.COLUMN_NAME,
                kcu2.TABLE_NAME,
                kcu2.COLUMN_NAME
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu1
                ON kcu1.CONSTRAINT_NAME = rc.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu2
                ON kcu2.CONSTRAINT_NAME = rc.UNIQUE_CONSTRAINT_NAME
            WHERE kcu1.TABLE_NAME = %s
            """,
            [table_name],
        )
        for constraint_name, col, ref_table, ref_col in cursor.fetchall():
            if constraint_name not in constraints:
                constraints[constraint_name] = {
                    "columns": [col],
                    "primary_key": False,
                    "unique": False,
                    "foreign_key": (ref_table, ref_col),
                    "check": False,
                    "index": False,
                    "definition": None,
                    "options": None,
                }
            else:
                constraints[constraint_name]["foreign_key"] = (ref_table, ref_col)

        # Indexes
        cursor.execute(
            """
            SELECT
                i.name AS index_name,
                c.name AS column_name,
                i.is_unique,
                i.is_primary_key
            FROM sys.indexes i
            JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            JOIN sys.objects o ON i.object_id = o.object_id
            WHERE o.name = %s AND i.name IS NOT NULL
            ORDER BY i.name, ic.key_ordinal
            """,
            [table_name],
        )
        for index_name, column_name, is_unique, is_primary in cursor.fetchall():
            if index_name not in constraints:
                constraints[index_name] = {
                    "columns": [],
                    "primary_key": bool(is_primary),
                    "unique": bool(is_unique),
                    "foreign_key": None,
                    "check": False,
                    "index": not is_primary,
                    "definition": None,
                    "options": None,
                }
            constraints[index_name]["columns"].append(column_name)

        return constraints
