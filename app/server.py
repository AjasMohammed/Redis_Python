import asyncio
import logging
import os
import socket
from .utilities import RedisProtocolParser, Store, DatabaseParser


logging.basicConfig(
    filename="main.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


class Server:
    def __init__(self, config):
        self.config = config
        self.store = Store()
        self.db = DatabaseParser()
        self.parser = RedisProtocolParser()

    async def start_server(self):
        server = await asyncio.start_server(
            self.handle_client, "localhost", self.config.port, reuse_port=True
        )

        # set path to the .rdb file in the config
        path = os.path.join(self.config.dir, self.config.dbfilename)
        self.config.db_path = path

        self.db.update_store(self.store, path)

        if self.config.replication.role == "slave":
            await self.create_handshake()

        # Serve clients indefinitely
        async with server:
            await server.serve_forever()

    # Define coroutine to handle client connections
    async def handle_client(self, reader, writer):
        logging.info(f"CLIENT CONNECTED :- {writer.get_extra_info('peername')}")
        pong = b"+PONG\r\n"
        while True:
            # Read data from the client
            data = await reader.read(1024)
            logging.debug(f"Recived data: {data}")
            if not data:
                break

            byte_data = self.parser.decoder(data)
            logging.debug(f"bytes data is {byte_data}")

            result = await self.handle_command(byte_data)
            if isinstance(result, bytes):
                encoded = result
            else:
                encoded = self.parser.encoder(result)
            logging.debug(f"ENCODED DATA : {encoded}")
            if encoded:
                writer.write(encoded)
            else:
                # Send PONG self.parseronse back to the client
                writer.write(pong)
            await writer.drain()
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
            return "OK"
        elif keyword == "PSYNC":
            response = self.parser.simple_string(
                self.config.replication.psync(), encode=True
            )
            print('Response : ', response)
            return bytes(response, 'utf-8')
        else:
            return None

    async def create_handshake(self):
        master_host = self.config.replication.master_host
        master_port = self.config.replication.master_port
        current_port = self.config.port

        master = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        master.connect((master_host, int(master_port)))

        # STEP - 1
        cmd = ["PING"]
        master.sendall(self.parser.encoder(cmd))
        response = master.recv(10254).decode("utf-8")
        logging.debug(f"Handshake STEP - 1 Response : {response}")

        # STEP - 2
        cmd = ["REPLCONF", "listening-port", str(current_port)]
        master.sendall(self.parser.encoder(cmd))
        response = master.recv(10254).decode("utf-8")
        logging.debug(f"Handshake STEP - 2 Response : {response}")

        cmd = ["REPLCONF", "capa", "psync2"]
        master.sendall(self.parser.encoder(cmd))
        response = master.recv(10254).decode("utf-8")
        logging.debug(f"Handshake STEP - 2.5 Response : {response}")

        # STEP - 3
        cmd = ["PSYNC", "?", "-1"]
        master.sendall(self.parser.encoder(cmd))
        response = master.recv(10254).decode("utf-8")
        logging.debug(f"Handshake STEP - 3 Response : {response}")

        return

    @staticmethod
    def check_index(keyword, array):
        for i in range(len(array)):
            if keyword.lower() == array[i].lower():
                return i
        return None
