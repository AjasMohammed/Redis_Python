import asyncio, traceback, logging
from app.utilities import (
    DatabaseParser,
    Store,
    ServerConfiguration,
    RedisProtocolParser,
    Replica,
)


class CommandHandler:
    def __init__(
        self,
        store: Store,
        db: DatabaseParser,
        config: ServerConfiguration,
    ):
        self.store: Store = store
        self.db: DatabaseParser = db
        self.config: ServerConfiguration = config
        self.replica_offset: asyncio.Condition = asyncio.Condition()
        self.updated_replicas: int = 0
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
            print(f"Client {client} connected")
            await self.create_replica(client, reader, writer)

        elif args[0].lower() == "getack":
            offset = self.config.replication.master_repl_offset
            print("Command Offset : ", offset)
            self.config.replication.master_repl_offset += 37
            return ["REPLCONF", "ACK", str(offset)]
        elif args[0].lower() == "ack":
            self.updated_replicas = 0
            for slave in self.config.replication._slaves_list:
                print(
                    f"Sende_Bytes to {slave.host}:{slave.port} : ",
                    self.config.replication.master_repl_offset,
                )
                if int(args[1]) >= (
                    int(self.config.replication.master_repl_offset) - 37
                ):
                    self.updated_replicas += 1
                print(f"Recived_Bytes from {slave.host}:{slave.port} : ", args[1])
            async with self.replica_offset:
                self.replica_offset.notify_all()

        return "OK"

    async def _psync(self, args, **kwargs):
        response = RedisProtocolParser.simple_string(
            self.config.replication.psync(), encode=True
        )
        empty_rdb = self.config.replication.empty_rdb()
        ack = b"*3\r\n$8\r\nreplconf\r\n$6\r\ngetack\r\n$1\r\n*\r\n"

        return (response.encode("utf-8"), empty_rdb)

    async def _wait(self, args, **kwargs):
        numreplicas = int(args[0])
        timeout = int(args[1]) / 1000
        total_replicas = len(self.config.replication._slaves_list)
        print("Master Repl Offset: ", self.config.replication.master_repl_offset)
        if self.config.replication.master_repl_offset == 0:
            return total_replicas
        else:
            try:
                print(f"Timeout : {timeout}")
                await asyncio.wait_for(
                    self.wait_for_replicas(numreplicas, timeout), timeout
                )
                # await self.wait_for_replicas(numreplicas)
            # except asyncio.TimeoutError:
            #     pass
            except TimeoutError:
                print("TimeoutError")
                pass
            except Exception as e:
                print(e)
                print(traceback.print_tb(e.__traceback__))
                logging.error(traceback.print_tb(e.__traceback__))
        return self.updated_replicas

    async def call_cmd(self, data, **kwargs):
        keyword, *args = data
        cmd = keyword.upper()
        try:
            if cmd in self.cmds:
                response = await self.cmds[cmd](args, **kwargs)
                await self.start_propagation(data)
                return response
            else:
                print("Command not found...")
        except Exception as e:
            print(e)
            print(traceback.print_tb(e.__traceback__))
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
        # self.config.slave_tasks.append(
        #     asyncio.create_task(self.propagate_to_slave(replica))
        # )

    async def start_propagation(self, command):
        data = RedisProtocolParser().encoder(command)
        if self.is_writable(command):
            self.calculate_bytes(data)
            if self.config.replication.role == "master":
                # self.config.replication.master_repl_offset += len(data)
                print(f"PROPAGATING: {command}")
                for slave in self.config.replication._slaves_list:
                    print("Start Propagation")
                    await self.propagate_to_slave(slave, data)

    async def propagate_to_slave(self, replica: Replica, data) -> None:
        # data = await replica.buffer_queue.get()
        print(f" DATA : {data}")
        try:
            replica.writer.write(data)
            replica.send_bytes += len(data)
            await replica.writer.drain()
        except Exception as e:
            print(e)
            print(traceback.print_tb(e.__traceback__))

    async def request_replica_offset(self):
        cmd = ["REPLCONF", "GETACK", "*"]
        data = RedisProtocolParser().encoder(cmd)
        for slave in self.config.replication._slaves_list:
            # print("Sende_Bytes : ", slave.send_bytes)
            slave.writer.write(data)
            await slave.writer.drain()
        self.config.replication.master_repl_offset += len(data)

    async def wait_for_replicas(self, numreplicas: int, timeout: float) -> None:
        # while True:
        if self.updated_replicas >= numreplicas:
            print("All slaves connected")
            return
        await self.request_replica_offset()
        async with self.replica_offset:
            await self.replica_offset.wait()
        if self.updated_replicas >= numreplicas:
            print("All slaves connected")
            return
        await asyncio.sleep(timeout)

    def calculate_bytes(self, data: bytes) -> int:
        print(f"Current offset : {self.config.replication.master_repl_offset}")
        self.config.replication.master_repl_offset += len(data)
        print(f"New offset : {self.config.replication.master_repl_offset}")

    @staticmethod
    def check_index(keyword, array):
        for i in range(len(array)):
            if keyword.lower() == array[i].lower():
                return i
        return None

    @staticmethod
    def is_writable(cmd):
        writable_cmd = [
            "SET",
            "GETSET",
            "DEL",
            "INCR",
            "DECR",
            "INCRBY",
            "DECRBY",
            "APPEND",
            "SETBIT",
            "SETEX",
            "MSET",
            "MSETNX",
            "HSET",
            "HSETNX",
            "HMSET",
        ]
        try:
            res = cmd[0].upper() in writable_cmd
            return res
        except AttributeError:
            res = []
            for i in cmd:
                res.append(i[0].upper() in writable_cmd)
            return all(res)
