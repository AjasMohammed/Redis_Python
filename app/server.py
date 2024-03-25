import asyncio
import logging
import os
import traceback
from .utilities import (
    CommandHandler,
    DatabaseParser,
    RedisProtocolParser,
    Store,
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
        self.cmd: CommandHandler

        self.server_writer: asyncio.StreamWriter = None
        self.server_reader: asyncio.StreamReader = None

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
        self.server_reader, self.server_writer = reader, writer

        checkclient = writer.get_extra_info("peername")
        logging.info(f"Peer : {checkclient}")
        pong = b"+PONG\r\n"
        while True:
            try:
                # Read data from the client
                data = await reader.read(1024)
                # data = await self.listen_server()
                logging.debug(f"Recived data: {data}")
                print(f"Recived data: {data} From {checkclient}")
                if not data:
                    break
                decoded_data = self.parser.decoder(data)
                logging.debug(f"bytes data is {decoded_data}")
                print(f"bytes data is {decoded_data}")

                if decoded_data:
                    response = await self.handle_command(decoded_data)
                    print("RESULT: ", response)

                    if isinstance(response, tuple):
                        for item in response:
                            await self.write_to_client(item, writer)
                    else:
                        await self.write_to_client(response, writer)
                    if self.config.replication.role == "master" and self.is_writable(
                        decoded_data
                    ):
                        print("propagating to slave")
                        for slave in self.config.replication._slaves_list:
                            await slave.buffer_queue.put(data)
            # Close the connection
            except Exception as e:
                print("Error in handle_client")
                print(e)
                logging.error(traceback.print_tb(e.__traceback__))
                break
        writer.close()

    async def listen_server(self) -> None:
        print("Listening Server")
        data = await self.server_reader.read(1024)
        print(f"Reading Data: {data}")
        return data

    async def handle_command(self, data) -> list | tuple:
        logging.debug(f"data is {data}")
        print(f"data is {data}")
        if isinstance(data[0], list) and len(data) > 1:
            res = []
            for cmd in data:
                keyword, *args = cmd
                keyword = keyword.upper()
                response = await self.cmd.call_cmd(
                    keyword, args, reader=self.server_reader, writer=self.server_writer
                )
                byte_data = self.parser.encoder(response)
                if (
                    self.config.replication.role == "slave"
                    and response
                    and "ACK" not in response
                ):
                    self.calculate_bytes(self.parser.encoder(cmd))
                res.append(byte_data)
                # await self.write_to_client(byte_data)
            print("RES: ", res)
            return tuple(res)
        else:
            if isinstance(data[0], list):
                data = data[0]
            keyword, *args = data
            keyword = keyword.upper()
            response = await self.cmd.call_cmd(
                keyword, args, reader=self.server_reader, writer=self.server_writer
            )
            if (
                self.config.replication.role == "slave"
                and response
                and "ACK" not in response
            ):
                self.calculate_bytes(self.parser.encoder(data))
            # await self.write_to_client(self.parser.encoder(response))
            encoded_data = self.parser.encoder(response)
            return encoded_data

    async def write_to_client(self, data, writer: asyncio.StreamWriter) -> None:

        print(f"Writing Data: {data}")
        try:
            if self.config.replication.role == "slave":
                master = (
                    self.config.replication.master_host,
                    int(self.config.replication.master_port),
                )
                client = writer.get_extra_info("peername")
                if client[0] == master[0] and client[1] == master[1]:
                    await self.should_respond(data)
                    print(f"Offset : {self.config.replication.command_offset}")
                    # data = await self.listen_server()
                    # print(f"New data : {data}")
                    return

            writer.write(data)
            await writer.drain()
            print("Wrote PONG response back to the client")
            # data = await self.listen_server()
            # decoded = self.parser.decoder(data)
            # print(f"New data : {data}")
            # await self.handle_command(decoded)
            return
        except Exception as e:
            print("Error in write_to_client")
            print(e)
            logging.error(traceback.print_tb(e.__traceback__))

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
            if ack_cmd:
                result = await self.handle_command(ack_cmd)
                print("ACK RESULT : ", result)
                encoded_data = self.parser.encoder(result)
                self.writer.write(encoded_data)
                await self.writer.drain()
        except Exception as e:
            logging.error(f"Handshake failed: {traceback.print_tb(e.__traceback__)}")

        finally:
            logging.info("Handshake Completed...")

    def calculate_bytes(self, data) -> int:
        print(f"Current offset : {self.config.replication.command_offset}")
        self.config.replication.command_offset += len(data)
        print(f"New offset : {self.config.replication.command_offset}")

    async def should_respond(self, data):
        try:
            if isinstance(data, tuple):
                for cmd in data:
                    if isinstance(cmd, str):
                        cmd = self.parser.encoder(cmd)
                    if b"ACK" in cmd:
                        self.server_writer.write(cmd)
                        await self.server_writer.drain()
            else:
                if isinstance(data, str):
                    data = self.parser.encoder(data)
                if b"ACK" in data:
                    self.server_writer.write(data)
                    await self.server_writer.drain()
        except Exception as e:
            print("Error in should_respond")
            print(e)
            logging.error(traceback.print_tb(e.__traceback__))

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
