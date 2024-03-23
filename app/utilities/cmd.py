from app.utilities import DatabaseParser, Store, ServerConfiguration, RedisProtocolParser


class CommandHandler:
    def __init__(self, store: Store, db: DatabaseParser, config: ServerConfiguration):
        self.store = store
        self.db = db
        self.config = config
        self.cmds = {
            "PING": self._ping,
            "SET": self._set_data,
            "GET": self._get_data,
            "ECHO": self._echo,
            "CONFIG": self._config,
            "KEYS": self._keys,
            "TYPE": self.type_,
            "XADD": self._xadd,
            "XRANGE": self._xrange,
            "XREAD": self._xread,
            "INFO": self._info,
            "REPLCONF": self._replconf,
            "PSYNC": self._psync,
        }

    def _ping(self, args):
        return "PONG"

    def _echo(self, args):
        return " ".join(args)

    def _set_data(self, args):
        if isinstance(args[0], list):
            while args:
                self._set_data(args.pop(0))
        else:
            key = args[0]
            value = args[1]
            args = args[2:]
            self.store.set(key, value, args)
            print(f"Setting key : {key} with value : {value}")
        return "OK"

    def _get_data(self, args):
        key = args[0]
        value = self.store.get(key)
        return value

    def _config(self, args):
        return self.config.handle_config(args)

    def _keys(self, args):
        if args[0] == "*":
            key_value_pair = self.db.key_value_pair
        else:
            key_value_pair = self.db.database_parser(path=self.config.db_path)
        if key_value_pair:
            return list(key_value_pair.keys())
        else:
            return None

    def type_(self, args):
        return self.store.type_check(args[0])

    def _xadd(self, args):
        key = args.pop(0)
        id = args.pop(0)
        self.parseronse = self.store.xadd(key, id, args)
        return self.parseronse

    def _xrange(self, args):
        key = args.pop(0)
        self.parseronse = self.store.xrange(key, args)
        return self.parseronse

    def _xread(self, args):
        block_ms = None
        block = self.check_index("BLOCK", args)
        if block != None:
            block_ms = int(args[block + 1])
            args = args[block + 2 :]
        streams = list(
            filter(
                lambda x: (x.isalpha() or x.isalnum() or "_" in x) and not x.isdigit(),
                args[1:],
            )
        )
        id = args[len(streams) + 1]
        self.parseronse = self.store.xread(streams, id, block_ms)
        return self.parseronse

    def _info(self, args):
        if args[0].lower() == "replication":
            rep = self.config.replication.view_info()
            return rep

    def _replconf(self, args):
        return "OK"

    def _psync(self, args):
        response = RedisProtocolParser.simple_string(
            self.config.replication.psync(), encode=True
        )
        empty_rdb = self.config.replication.empty_rdb()
        return (response.encode("utf-8"), empty_rdb)

    def call_cmd(self, cmd: str, args):
        cmd = cmd.upper()
        if cmd in self.cmds:
            return self.cmds[cmd](args)
        else:
            print("Command not found...")


# Usage example
if __name__ == "__main__":
    handler = CommandHandler()
    print(handler.call_cmd("_PING"))  # Output: PONG
    print(handler.call_cmd("SET", key="foo", value="bar", args=[]))  # Output: OK
    print(handler.call_cmd("GET", key="foo"))  # Output: bar
    print(handler.call_cmd("ECHO", data=["Hello", "World"]))  # Output: Hello World
