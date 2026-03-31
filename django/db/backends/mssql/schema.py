from django.db.backends.base.schema import BaseDatabaseSchemaEditor


class DatabaseSchemaEditor(BaseDatabaseSchemaEditor):
    sql_create_column = "ALTER TABLE %(table)s ADD %(column)s %(definition)s"
    sql_delete_column = "ALTER TABLE %(table)s DROP COLUMN %(column)s"
    sql_rename_column = (
        "EXEC sp_rename '%(table)s.%(old_column)s', '%(new_column)s', 'COLUMN'"
    )
    sql_rename_table = "EXEC sp_rename %(old_table)s, %(new_table)s"
    sql_delete_index = "DROP INDEX %(name)s ON %(table)s"
    sql_create_pk = (
        "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s PRIMARY KEY (%(columns)s)"
    )
    sql_delete_pk = "ALTER TABLE %(table)s DROP CONSTRAINT %(name)s"
    sql_create_fk = (
        "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s FOREIGN KEY (%(column)s) "
        "REFERENCES %(to_table)s (%(to_column)s)%(on_delete_db)s%(deferrable)s"
    )
    sql_delete_fk = "ALTER TABLE %(table)s DROP CONSTRAINT %(name)s"
    sql_create_unique = (
        "ALTER TABLE %(table)s ADD CONSTRAINT %(name)s UNIQUE (%(columns)s)"
    )
    sql_delete_unique = "ALTER TABLE %(table)s DROP CONSTRAINT %(name)s"

    def quote_value(self, value):
        if isinstance(value, str):
            return "'%s'" % value.replace("'", "''")
        if isinstance(value, bool):
            return "1" if value else "0"
        if value is None:
            return "NULL"
        return str(value)

    def _alter_column_type_sql(
        self, model, old_field, new_field, new_type, old_collation, new_collation
    ):
        # SQL Server: ALTER COLUMN doesn't support IDENTITY changes
        # Just do a basic type change
        new_internal_type = new_field.get_internal_type()
        old_internal_type = old_field.get_internal_type()
        auto_field_types = {"AutoField", "BigAutoField", "SmallAutoField"}

        if new_internal_type in auto_field_types or old_internal_type in auto_field_types:
            # Can't alter IDENTITY columns directly; skip
            return (
                (
                    "SELECT 1",  # no-op
                    [],
                ),
                [],
            )
        return super()._alter_column_type_sql(
            model, old_field, new_field, new_type, old_collation, new_collation
        )

    def _rename_field_sql(self, table, old_field, new_field, new_type):
        return self.sql_rename_column % {
            "table": self.quote_name(table),
            "old_column": self.quote_name(old_field.column),
            "new_column": self.quote_name(new_field.column),
        }

    def _delete_index_sql(self, model, name, sql=None):
        return self.sql_delete_index % {
            "name": self.quote_name(name),
            "table": self.quote_name(model._meta.db_table),
        }

    def _create_index_sql(
        self,
        model,
        *,
        fields=None,
        name=None,
        suffix="",
        using="",
        db_tablespace=None,
        col_suffixes=(),
        sql=None,
        opclasses=(),
        condition=None,
        concurrently=False,
        include=None,
        expressions=None,
    ):
        return super()._create_index_sql(
            model,
            fields=fields,
            name=name,
            suffix=suffix,
            using=using,
            db_tablespace=db_tablespace,
            col_suffixes=col_suffixes,
            sql=sql,
            opclasses=opclasses,
            condition=condition,
            include=include,
            expressions=expressions,
        )

    def effective_default(self, field):
        return super().effective_default(field)

    def column_sql(self, model, field, include_default=False):
        # SQL Server IDENTITY columns can't have a DEFAULT
        db_params = field.db_parameters(connection=self.connection)
        sql = db_params["type"]
        if sql is None:
            return None, None
        # IDENTITY columns don't need DEFAULT
        if "IDENTITY" in sql.upper():
            include_default = False
        return super().column_sql(model, field, include_default)
