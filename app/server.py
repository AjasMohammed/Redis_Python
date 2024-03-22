import asyncio
import logging
import os
import socket
from .utilities import (
    RedisProtocolParser,
    Store,
    DatabaseParser,
    Replica,
    ServerConfiguration,
)


logging.basicConfig(
    filename="main.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class Server:
    def __init__(self, config):
        self.config: ServerConfiguration = config
        self.store: Store = Store()
        self.db: DatabaseParser = DatabaseParser()
        self.parser: RedisProtocolParser = RedisProtocolParser()

        self.slaves: list[Replica] = []
        self.slave_tasks: list[asyncio.Task] = []
        self.writable_cmd = [
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

    async def start_server(self):
        server = await asyncio.start_server(
            self.handle_client, self.config.host, self.config.port
        )
        server.sockets[0].setblocking(False)
        logging.info(f"Serving on: {server.sockets[0].getsockname()}")
        # set path to the .rdb file in the config
        path = os.path.join(self.config.dir, self.config.dbfilename)
        self.config.db_path = path

        self.db.update_store(self.store, path)

        if self.config.replication.role == "slave":
            await self.handle_replication()
            await self.listen_master()

        # Serve clients indefinitely
        async with server:
            print("Start serving forever")
            await server.serve_forever()

    # Define coroutine to handle client connections

    async def handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ):
        client = writer.get_extra_info("peername")
        logging.info(f"Peer : {client}")
        pong = b"+PONG\r\n"
        while True:
            # Read data from the client
            data = await reader.read(1024)
            logging.debug(f"Recived data: {data}")
            print(f"Recived data: {data}")
            if not data:
                break

            byte_data = self.parser.decoder(data)
            logging.debug(f"bytes data is {byte_data}")
            print(f"bytes data is {byte_data}")

            if (
                byte_data
                and "replconf" == byte_data[0].lower()
                and client
                and byte_data[1] == "listening-port"
            ):
                replica = Replica(
                    host=client[0],
                    port=client[1],
                    reader=reader,
                    writer=writer,
                    buffer_queue=asyncio.Queue(),
                )
                self.slaves.append(replica)
                self.slave_tasks.append(
                    asyncio.create_task(self.propagate_to_slave(replica))
                )
                client = None

            if byte_data:
                result = await self.handle_command(byte_data)
                if isinstance(result, bytes | tuple):
                    encoded = result
                else:
                    encoded = self.parser.encoder(result)
                if encoded:
                    if isinstance(encoded, tuple):
                        data, rdb = encoded
                        writer.write(data)
                        await writer.drain()
                        writer.write(rdb)
                    else:
                        writer.write(encoded)
                else:
                    # Send PONG response back to the client
                    writer.write(pong)
                await writer.drain()

            if (
                self.config.replication.role == "master"
                and byte_data
                and byte_data[0].upper() in self.writable_cmd
            ):
                print("propagating to slave")
                for slave in self.slaves:
                    print(f"Saving data to queue : {slave}")
                    await slave.buffer_queue.put(data)
                    print(f"Slave Tasks : {self.slave_tasks}")

        # Close the connection
        writer.close()

    async def handle_command(self, data):
        logging.debug(f"data is {data}")
        keyword, *args = data
        keyword = keyword.upper()
        if keyword == "PING":
            return "PONG"
        elif keyword == "ECHO":
            return " ".join(args)
        elif keyword == "SET":
            self.store.set(args[0], args[1], args[2:])
            return "OK"
        elif keyword == "GET":
            data = self.store.get(args[0])
            return data
        elif keyword == "CONFIG":
            return self.config.handle_config(args)
        elif keyword == "KEYS":
            if args[0] == "*":
                key_value_pair = self.db.key_value_pair
            else:
                key_value_pair = self.db.database_parser(self.config.db_path)
            if key_value_pair:
                return list(key_value_pair.keys())
            else:
                return None
        elif keyword == "TYPE":
            value_type = self.store.type_check(args[0])
            return value_type
        elif keyword == "XADD":
            key = args.pop(0)
            id = args.pop(0)
            self.parseronse = self.store.xadd(key, id, args)
            return self.parseronse
        elif keyword == "XRANGE":
            key = args.pop(0)
            self.parseronse = self.store.xrange(key, args)
            return self.parseronse
        elif keyword == "XREAD":
            block_ms = None
            block = self.check_index("BLOCK", args)
            if block != None:
                block_ms = int(args[block + 1])
                args = args[block + 2 :]
            streams = list(
                filter(
                    lambda x: (x.isalpha() or x.isalnum() or "_" in x)
                    and not x.isdigit(),
                    args[1:],
                )
            )
            id = args[len(streams) + 1]
            self.parseronse = await self.store.xread(streams, id, block_ms)
            return self.parseronse
        elif keyword == "INFO":
            if args[0].lower() == "replication":
                rep = self.config.replication.view_info()
                return rep
        elif keyword == "REPLCONF":
            # if args[0].lower() == "listening-port":

            return "OK"
        elif keyword == "PSYNC":
            response = self.parser.simple_string(
                self.config.replication.psync(), encode=True
            )
            empty_rdb = self.config.replication.empty_rdb()
            return (bytes(response, "utf-8"), empty_rdb)
        else:
            return None

    async def listen_master(self):
        print("Listening Master")
        try:
            while True:
                try:
                    await self.handle_client(self.reader, self.writer)
                except ConnectionResetError:
                    print("Connection err")
                    return
        except asyncio.CancelledError:
            self.writer.close()
            await self.writer.wait_closed()

    async def handle_replication(self):
        master_host = self.config.replication.master_host
        master_port = self.config.replication.master_port
        current_port = self.config.port

        logging.info(f"Handshake with master: {master_host}:{master_port}")
        try:
            print(f"Connecting to {master_host}:{master_port}")
            self.reader, self.writer = await asyncio.open_connection(
                master_host, int(master_port)
            )

            # STEP - 1
            cmd = ["PING"]
            self.writer.write(self.parser.encoder(cmd))
            await self.writer.drain()
            response = await self.reader.read(1024)
            logging.info(f"Handshake STEP - 1 Response : {response.decode('utf-8')}")

            # STEP - 2
            cmd = ["REPLCONF", "listening-port", str(current_port)]
            self.writer.write(self.parser.encoder(cmd))
            await self.writer.drain()
            response = await self.reader.read(1024)
            logging.info(f"Handshake STEP - 2 Response : {response.decode('utf-8')}")

            cmd = ["REPLCONF", "capa", "psync2"]
            self.writer.write(self.parser.encoder(cmd))
            await self.writer.drain()
            response = await self.reader.read(1024)
            logging.info(f"Handshake STEP - 2.5 Response : {response.decode('utf-8')}")

            # STEP - 3
            cmd = ["PSYNC", "?", "-1"]
            self.writer.write(self.parser.encoder(cmd))
            await self.writer.drain()
            response = await self.reader.read(1024)
            logging.info(f"Handshake STEP - 3 Response : {response}")

        except Exception as e:
            logging.error(f"Handshake failed: {e}")

        finally:
            logging.info("Handshake Completed...")

    async def propagate_to_slave(self, replica: Replica):
        print(
            f"Start background task to send write commands to {replica.host}:{replica.port}"
        )
        while True:
            data = await replica.buffer_queue.get()
            print(f" DATA : {data}")
            # if self.config.replication.role == "master" and self.slaves:
            #     for slave in self.slaves:
            try:
                replica.writer.write(data)
                await replica.writer.drain()
                # reader, writer = await self.create_slave_connection(slave)
                # writer.write(data)
                # r = await reader.read(1024)
                # print("READ : ", r)
                # writer.close()
            except Exception as e:
                print(e)

    @staticmethod
    def check_index(keyword, array):
        for i in range(len(array)):
            if keyword.lower() == array[i].lower():
                return i
        return None
