from django.db.backends.base.client import BaseDatabaseClient


class DatabaseClient(BaseDatabaseClient):
    executable_name = "sqlcmd"

    @classmethod
    def settings_to_cmd_args_env(cls, settings_dict, parameters):
        args = [cls.executable_name]
        host = settings_dict.get("HOST", "127.0.0.1")
        port = settings_dict.get("PORT")
        dbname = settings_dict.get("NAME", "")
        user = settings_dict.get("USER", "")
        passwd = settings_dict.get("PASSWORD", "")

        if host:
            server = f"{host},{port}" if port else host
            args += ["-S", server]
        if dbname:
            args += ["-d", dbname]
        if user:
            args += ["-U", user]
        if passwd:
            args += ["-P", passwd]

        args.extend(parameters)
        return args, None

    def runshell(self, parameters):
        super().runshell(parameters)
