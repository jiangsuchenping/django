import sys

from django.db.backends.base.creation import BaseDatabaseCreation


class DatabaseCreation(BaseDatabaseCreation):
    def _quote_name(self, name):
        return self.connection.ops.quote_name(name)

    def sql_table_creation_suffix(self):
        return ""

    def _database_exists(self, cursor, database_name):
        # Strip brackets if quoted
        db_name = database_name.strip("[]")
        cursor.execute(
            "SELECT 1 FROM sys.databases WHERE name = %s",
            [db_name],
        )
        return cursor.fetchone() is not None

    def _execute_create_test_db(self, cursor, parameters, keepdb=False):
        if keepdb and self._database_exists(cursor, parameters["dbname"]):
            return
        try:
            super()._execute_create_test_db(cursor, parameters, keepdb)
        except Exception as e:
            if "already exists" not in str(e):
                self.log("Got an error creating the test database: %s" % e)
                sys.exit(2)
            elif not keepdb:
                raise

    def _destroy_test_db(self, test_database_name, verbosity):
        # Close all connections to the database before dropping
        with self._nodb_cursor() as cursor:
            db_name = test_database_name.strip("[]")
            cursor.execute(
                """
                ALTER DATABASE %s SET SINGLE_USER WITH ROLLBACK IMMEDIATE
                """ % self._quote_name(db_name)
            )
            cursor.execute("DROP DATABASE %s" % self._quote_name(db_name))

    def _get_test_db_name(self):
        return self.connection.settings_dict["TEST"].get(
            "NAME"
        ) or "test_%s" % self.connection.settings_dict["NAME"]
