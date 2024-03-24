import asyncio
import logging
import os
import sys
from .utilities import (
    RedisProtocolParser,
    Store,
    DatabaseParser,
    Replica,
    ServerConfiguration,
    CommandHandler,
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
        self.cmd: CommandHandler

        self.slaves: list[Replica] = []
        self.slave_tasks: list[asyncio.Task] = []

    async def start_server(self):
        server = await asyncio.start_server(
            self.handle_client, self.config.host, self.config.port
        )
        server.sockets[0].setblocking(False)
        logging.info(f"Serving on: {server.sockets[0].getsockname()}")

        # set path to the .rdb file in the config
        path = os.path.join(self.config.dir, self.config.dbfilename)
        self.config.db_path = path
        self.db.update_store(self.store, path=path)

        self.cmd = CommandHandler(self.store, self.db, self.config)

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
        if self.config.replication.role == "slave":
            master = (
                self.config.replication.master_host,
                int(self.config.replication.master_port),
            )
        client = writer.get_extra_info("peername")
        checkclient = writer.get_extra_info("peername")
        logging.info(f"Peer : {client}")
        pong = b"+PONG\r\n"
        while True:
            try:
                print("Waiting for data")
                # Read data from the client
                data = await reader.read(1024)
                logging.debug(f"Recived data: {data}")
                print(f"Recived data: {data} From {checkclient}")
                if not data:
                    break
                print("Decoding...")
                print("Data : ", data)
                byte_data = self.parser.decoder(data)

                logging.debug(f"bytes data is {byte_data}")
                print(f"bytes data is {byte_data}")

                if (
                    byte_data
                    and isinstance(byte_data[0], str)
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

                if byte_data:
                    result = await self.handle_command(byte_data)
                    print("RESULT: ", result)

                    if (
                        self.config.replication.role == "slave"
                        and client[0] == master[0]
                        and client[1] == master[1]
                    ):
                        await self.should_respond(result, writer)
                        print(f"Offset : {self.config.replication.command_offset}")
                        print(byte_data)
                        # self.config.replication.command_offset += len(data)

                    else:
                        if isinstance(result, bytes | tuple):
                            encoded = result
                        else:
                            encoded = self.parser.encoder(result)
                        print(f"Encoded : {encoded}")
                        if encoded:
                            if isinstance(encoded, tuple):
                                for item in encoded:
                                    writer.write(item)
                                    await writer.drain()
                            else:
                                writer.write(encoded)
                                await writer.drain()
                        else:
                            # Send PONG response back to the client
                            writer.write(pong)
                            await writer.drain()
                        if (
                            self.config.replication.role == "master"
                            and self.is_writable(byte_data)
                        ):
                            print("propagating to slave")
                            for slave in self.slaves:
                                # print(f"Saving data to queue : {slave}")
                                await slave.buffer_queue.put(data)
            # Close the connection
            except Exception as e:
                print(e)
                print(self.config.replication.role)
                break
        writer.close()

    async def handle_command(self, data) -> list | tuple:
        logging.debug(f"data is {data}")
        print(f"data is {data}")
        if isinstance(data[0], list) and len(data) > 1:
            res = []
            for cmd in data:
                keyword, *args = cmd
                keyword = keyword.upper()
                response = await self.cmd.call_cmd(keyword, args)
                byte_data = self.parser.encoder(response)
                if (
                    self.config.replication.role == "slave"
                    and response
                    and "ACK" not in response
                ):
                    self.calculate_bytes(self.parser.encoder(cmd))
                res.append(byte_data)
            print("RES: ", res)
            return tuple(res)
        else:
            if isinstance(data[0], list):
                data = data[0]
            keyword, *args = data
            keyword = keyword.upper()
            response = await self.cmd.call_cmd(keyword, args)
            if (
                self.config.replication.role == "slave"
                and response
                and "ACK" not in response
            ):
                self.calculate_bytes(self.parser.encoder(data))
            return response

    async def listen_master(self) -> None:
        print("Listening Master")

        try:
            await self.handle_client(self.reader, self.writer)
        except ConnectionResetError:
            print("Connection err")
            return
        except asyncio.CancelledError:
            self.writer.close()
            await self.writer.wait_closed()

    async def handle_replication(self) -> None:
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
            print(f"Handshake STEP - 1 Response : {response.decode('utf-8')}")

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
            print(f"Handshake STEP - 3 Response : {response}")

            resp_data = response[:]
            master_info = resp_data.split(b"\r\n", 1).pop(0)
            master_info = self.parser.decoder(master_info + b"\r\n")
            print("RESPONSE: ", master_info)
            index = resp_data.find(b"*")
            ack_cmd = self.parser.decoder(resp_data[index:])
            print("ACK CMD : ", ack_cmd)
            result = await self.handle_command(ack_cmd)
            print("ACK RESULT : ", result)
            encoded_data = self.parser.encoder(result)
            self.writer.write(encoded_data)
            await self.writer.drain()
        except Exception as e:
            logging.error(f"Handshake failed: {e}")

        finally:
            logging.info("Handshake Completed...")

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

    def calculate_bytes(self, data) -> int:
        print(f"Current offset : {self.config.replication.command_offset}")
        self.config.replication.command_offset += len(data)
        print(f"New offset : {self.config.replication.command_offset}")

    async def should_respond(self, data, writer):
        if isinstance(data, tuple):
            for cmd in data:
                if "ACK" in cmd:
                    cmd = self.encoder(cmd)
                    writer.write(cmd)
                    await writer.drain()
        else:
            if "ACK" in data:
                data = self.encoder(data)
                writer.write(data)
                await writer.drain()

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
