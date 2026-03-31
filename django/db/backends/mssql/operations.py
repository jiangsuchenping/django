import datetime
import uuid

from django.conf import settings
from django.db.backends.base.operations import BaseDatabaseOperations
from django.db.models.constants import OnConflict
from django.utils.timezone import utc


class DatabaseOperations(BaseDatabaseOperations):
    compiler_module = "django.db.backends.mssql.compiler"
    cast_char_field_without_max_length = "nvarchar(max)"
    explain_prefix = "SET STATISTICS"

    cast_data_types = {
        "AutoField": "int",
        "BigAutoField": "bigint",
        "SmallAutoField": "smallint",
        "CharField": "nvarchar(%(max_length)s)",
        "TextField": "nvarchar(max)",
    }

    def bulk_batch_size(self, fields, objs):
        # SQL Server has a limit of 2100 parameters per query
        if len(fields) == 0:
            return len(objs)
        return max(1, 2000 // len(fields))

    def cache_key_culling_sql(self):
        return "SELECT cache_key FROM %s ORDER BY cache_key OFFSET %%s ROWS FETCH NEXT 1 ROWS ONLY"

    def date_extract_sql(self, lookup_type, sql, params):
        if lookup_type == "week_day":
            # Sunday=1, Saturday=7 (DATEPART returns 1=Sunday by default)
            return f"DATEPART(weekday, {sql})", params
        elif lookup_type == "iso_week_day":
            # Monday=1, Sunday=7
            return f"(DATEPART(weekday, {sql}) + 5) %% 7 + 1", params
        elif lookup_type == "week":
            return f"DATEPART(iso_week, {sql})", params
        elif lookup_type == "iso_year":
            return f"YEAR(DATEADD(day, 26 - DATEPART(iso_week, {sql}), {sql}))", params
        elif lookup_type == "quarter":
            return f"DATEPART(quarter, {sql})", params
        lookup_map = {
            "year": "year",
            "month": "month",
            "day": "day",
            "hour": "hour",
            "minute": "minute",
            "second": "second",
        }
        part = lookup_map.get(lookup_type, lookup_type)
        return f"DATEPART({part}, {sql})", params

    def date_trunc_sql(self, lookup_type, sql, params, tzname=None):
        # SQL Server doesn't have DATE_TRUNC, simulate it
        if lookup_type == "year":
            return f"DATEFROMPARTS(YEAR({sql}), 1, 1)", params
        elif lookup_type == "quarter":
            return (
                f"DATEFROMPARTS(YEAR({sql}), (DATEPART(quarter, {sql}) - 1) * 3 + 1, 1)",
                params,
            )
        elif lookup_type == "month":
            return f"DATEFROMPARTS(YEAR({sql}), MONTH({sql}), 1)", params
        elif lookup_type == "week":
            return (
                f"DATEADD(day, 1 - DATEPART(weekday, {sql}), CAST({sql} AS date))",
                params,
            )
        elif lookup_type == "day":
            return f"CAST({sql} AS date)", params
        return sql, params

    def datetime_cast_date_sql(self, sql, params, tzname):
        return f"CAST({sql} AS date)", params

    def datetime_cast_time_sql(self, sql, params, tzname):
        return f"CAST({sql} AS time)", params

    def datetime_extract_sql(self, lookup_type, sql, params, tzname):
        return self.date_extract_sql(lookup_type, sql, params)

    def datetime_trunc_sql(self, lookup_type, sql, params, tzname):
        if lookup_type == "year":
            return f"CAST(DATEFROMPARTS(YEAR({sql}), 1, 1) AS datetime2)", params
        elif lookup_type == "quarter":
            return (
                f"CAST(DATEFROMPARTS(YEAR({sql}), (DATEPART(quarter, {sql}) - 1) * 3 + 1, 1) AS datetime2)",
                params,
            )
        elif lookup_type == "month":
            return (
                f"CAST(DATEFROMPARTS(YEAR({sql}), MONTH({sql}), 1) AS datetime2)",
                params,
            )
        elif lookup_type == "week":
            return (
                f"CAST(DATEADD(day, 1 - DATEPART(weekday, {sql}), CAST({sql} AS date)) AS datetime2)",
                params,
            )
        elif lookup_type == "day":
            return f"CAST(CAST({sql} AS date) AS datetime2)", params
        elif lookup_type == "hour":
            return (
                f"DATEADD(hour, DATEPART(hour, {sql}), CAST(CAST({sql} AS date) AS datetime2))",
                params,
            )
        elif lookup_type == "minute":
            return (
                f"DATEADD(minute, DATEPART(minute, {sql}), DATEADD(hour, DATEPART(hour, {sql}), CAST(CAST({sql} AS date) AS datetime2)))",
                params,
            )
        elif lookup_type == "second":
            return (
                f"DATEADD(second, DATEPART(second, {sql}), DATEADD(minute, DATEPART(minute, {sql}), DATEADD(hour, DATEPART(hour, {sql}), CAST(CAST({sql} AS date) AS datetime2))))",
                params,
            )
        return sql, params

    def time_extract_sql(self, lookup_type, sql, params):
        return self.date_extract_sql(lookup_type, sql, params)

    def time_trunc_sql(self, lookup_type, sql, params, tzname=None):
        if lookup_type == "hour":
            return f"TIMEFROMPARTS(DATEPART(hour, {sql}), 0, 0, 0, 0)", params
        elif lookup_type == "minute":
            return (
                f"TIMEFROMPARTS(DATEPART(hour, {sql}), DATEPART(minute, {sql}), 0, 0, 0)",
                params,
            )
        elif lookup_type == "second":
            return (
                f"TIMEFROMPARTS(DATEPART(hour, {sql}), DATEPART(minute, {sql}), DATEPART(second, {sql}), 0, 0)",
                params,
            )
        return sql, params

    def no_limit_value(self):
        return None

    def limit_offset_sql(self, low_mark, high_mark):
        # SQL Server uses OFFSET ... FETCH NEXT ... ROWS ONLY
        if high_mark is None:
            # No limit
            if low_mark:
                return f"OFFSET {low_mark} ROWS"
            return ""
        limit = high_mark - low_mark
        if low_mark:
            return f"OFFSET {low_mark} ROWS FETCH NEXT {limit} ROWS ONLY"
        return f"OFFSET 0 ROWS FETCH NEXT {limit} ROWS ONLY"

    def quote_name(self, name):
        if name.startswith("[") and name.endswith("]"):
            return name
        return "[%s]" % name

    def sql_flush(self, style, tables, *, reset_sequences=False, allow_cascade=False):
        if not tables:
            return []
        sql = []
        if allow_cascade:
            # Disable FK constraints temporarily
            sql.append("EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT ALL'")
        for table in tables:
            sql.append(
                "%s %s"
                % (
                    style.SQL_KEYWORD("TRUNCATE TABLE"),
                    style.SQL_FIELD(self.quote_name(table)),
                )
            )
        if allow_cascade:
            sql.append("EXEC sp_MSforeachtable 'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT ALL'")
        if reset_sequences:
            for table in tables:
                sql.append(
                    "DBCC CHECKIDENT (%s, RESEED, 0)" % self.quote_name(table)
                )
        return sql

    def sequence_reset_sql(self, style, model_list):
        # SQL Server uses IDENTITY columns, reset via DBCC CHECKIDENT
        from django.db import models

        output = []
        for model in model_list:
            for f in model._meta.local_fields:
                if isinstance(f, models.AutoField):
                    output.append(
                        "DBCC CHECKIDENT (%s, RESEED, 0);"
                        % self.quote_name(model._meta.db_table)
                    )
                    break
        return output

    def max_name_length(self):
        return 128

    def pk_default_value(self):
        return "DEFAULT"

    def prep_for_iexact_query(self, x):
        return x

    def adapt_datetimefield_value(self, value):
        if value is None:
            return None
        if hasattr(value, "resolve_expression"):
            return value
        if settings.USE_TZ and isinstance(value, datetime.datetime):
            if value.tzinfo is not None:
                value = value.astimezone(utc).replace(tzinfo=None)
        return value

    def adapt_timefield_value(self, value):
        if value is None:
            return None
        if hasattr(value, "resolve_expression"):
            return value
        if isinstance(value, str):
            return value
        return value.replace(microsecond=0) if isinstance(value, datetime.time) else value

    def adapt_decimalfield_value(self, value, max_digits=None, decimal_places=None):
        return value

    def adapt_ipaddressfield_value(self, value):
        return value or None

    def adapt_json_value(self, value, encoder):
        import json
        if encoder is None:
            return json.dumps(value)
        return json.dumps(value, cls=encoder)

    def last_insert_id(self, cursor, table_name, pk_name):
        cursor.execute("SELECT CAST(SCOPE_IDENTITY() AS bigint)")
        row = cursor.fetchone()
        if row is None:
            return None
        return row[0]

    def fetch_returned_insert_columns(self, cursor, returning_params):
        return cursor.fetchall()

    def return_insert_columns(self, fields):
        if not fields:
            return "", ()
        columns = [
            "%s.%s" % (
                self.quote_name("inserted"),
                self.quote_name(field.column),
            )
            for field in fields
        ]
        return "OUTPUT %s" % ", ".join(columns), ()

    def lookup_cast(self, lookup_type, internal_type=None):
        if lookup_type in ("iexact", "icontains", "istartswith", "iendswith"):
            return "UPPER(%s)"
        return "%s"

    def distinct_sql(self, fields, params):
        if fields:
            raise NotImplementedError(
                "DISTINCT ON fields is not supported by SQL Server."
            )
        return ["DISTINCT"], []

    def last_executed_query(self, cursor, sql, params):
        return super().last_executed_query(cursor, sql, params)

    def subtract_temporals(self, internal_type, lhs, rhs):
        lhs_sql, lhs_params = lhs
        rhs_sql, rhs_params = rhs
        params = (*lhs_params, *rhs_params)
        if internal_type == "TimeField":
            return (
                "DATEDIFF_BIG(microsecond, %s, %s) * 1000" % (rhs_sql, lhs_sql),
                params,
            )
        return (
            "DATEDIFF_BIG(microsecond, %s, %s) * 1000" % (rhs_sql, lhs_sql),
            params,
        )

    def combine_expression(self, connector, sub_expressions):
        if connector == "%%":
            return "MOD(%s)" % ", ".join(sub_expressions)
        return super().combine_expression(connector, sub_expressions)

    def integer_field_range(self, internal_type):
        ranges = {
            "SmallIntegerField": (-32768, 32767),
            "IntegerField": (-2147483648, 2147483647),
            "BigIntegerField": (-9223372036854775808, 9223372036854775807),
            "PositiveSmallIntegerField": (0, 32767),
            "PositiveIntegerField": (0, 2147483647),
            "PositiveBigIntegerField": (0, 9223372036854775807),
            "SmallAutoField": (-32768, 32767),
            "AutoField": (-2147483648, 2147483647),
            "BigAutoField": (-9223372036854775808, 9223372036854775807),
        }
        return ranges.get(internal_type, (None, None))
