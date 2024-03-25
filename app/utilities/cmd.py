import asyncio
from app.utilities import (
    DatabaseParser,
    Store,
    ServerConfiguration,
    RedisProtocolParser,
    Replica,
)


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
            "WAIT": self._wait,
        }

    async def _ping(self, args, **kwargs):
        return "PONG"

    async def _echo(self, args, **kwargs):
        return " ".join(args)

    async def _set_data(self, args, **kwargs):
        key = args[0]
        value = args[1]
        args = args[2:]
        self.store.set(key, value, args)
        print(f"Setting key : {key} with value : {value}")
        return "OK"

    async def _get_data(self, args, **kwargs):
        key = args[0]
        value = self.store.get(key)
        return value

    async def _config(self, args, **kwargs):
        return self.config.handle_config(args)

    async def _keys(self, args, **kwargs):
        if args[0] == "*":
            key_value_pair = self.db.key_value_pair
        else:
            key_value_pair = self.db.database_parser(path=self.config.db_path)
        if key_value_pair:
            return list(key_value_pair.keys())
        else:
            return None

    async def type_(self, args, **kwargs):
        return self.store.type_check(args[0])

    async def _xadd(self, args, **kwargs):
        key = args.pop(0)
        id = args.pop(0)
        response = self.store.xadd(key, id, args)
        return response

    async def _xrange(self, args, **kwargs):
        key = args.pop(0)
        response = self.store.xrange(key, args)
        return response

    async def _xread(self, args, **kwargs):
        print("XREAD")
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
        response = await self.store.xread(streams, id, block_ms)
        return response

    async def _info(self, args, **kwargs):
        if args[0].lower() == "replication":
            rep = self.config.replication.view_info()
            return rep

    async def _replconf(self, args, **kwargs):
        if args[0].lower() == "listening-port":
            writer = kwargs["writer"]
            reader = kwargs["reader"]
            client = writer.get_extra_info("peername")
            await self.create_replica(client, reader, writer)

        elif args[0].lower() == "getack":
            offset = self.config.replication.command_offset
            print("Commande Offset : ", offset)
            self.config.replication.command_offset += 37
            return ["REPLCONF", "ACK", str(offset)]
        return "OK"

    async def _psync(self, args, **kwargs):
        response = RedisProtocolParser.simple_string(
            self.config.replication.psync(), encode=True
        )
        empty_rdb = self.config.replication.empty_rdb()
        ack = b"*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n"
        writer = kwargs["writer"]
        writer.write(response.encode("utf-8"))
        await writer.drain()
        writer.write(empty_rdb)
        await writer.drain()
        return

    async def _wait(self, args, **kwargs):
        numreplicas = int(args[0])
        timeout = int(args[1])
        total_replicas = len(self.config.replication.slaves)
        # if total_replicas < numreplicas:
        #     return total_replicas
        return total_replicas

    async def call_cmd(self, cmd: str, args, **kwargs):
        cmd = cmd.upper()
        print("CMD: ", cmd)
        try:
            if cmd in self.cmds:
                response = await self.cmds[cmd](args, **kwargs)
                print('CMD RESPONSE : ', response)
                return response
            else:
                print("Command not found...")
        except Exception as e:
            print(e)
            return None

    async def create_replica(self, client, reader, writer):
        replica = Replica(
            host=client[0],
            port=client[1],
            reader=reader,
            writer=writer,
            buffer_queue=asyncio.Queue(),
        )
        self.config.replication.add_slave(replica)
        self.config.slave_tasks.append(
            asyncio.create_task(self.propagate_to_slave(replica))
        )

    async def propagate_to_slave(self, replica: Replica) -> None:
        print(
            f"Start background task to send write commands to {replica.host}:{replica.port}"
        )
        while True:
            data = await replica.buffer_queue.get()
            print(f" DATA : {data}")
            try:
                replica.writer.write(data)
                await replica.writer.drain()
            except Exception as e:
                print(e)

    @staticmethod
    def check_index(keyword, array):
        for i in range(len(array)):
            if keyword.lower() == array[i].lower():
                return i
        return None


# Usage example
if __name__ == "__main__":
    handler = CommandHandler()
    print(handler.call_cmd("_PING"))  # Output: PONG
    print(handler.call_cmd("SET", key="foo", value="bar", args=[]))  # Output: OK
    print(handler.call_cmd("GET", key="foo"))  # Output: bar
    print(handler.call_cmd("ECHO", data=["Hello", "World"]))  # Output: Hello World
