import asyncio
import logging
import os
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

    async def start_server(self):
        server = await asyncio.start_server(
            self.handle_client, "localhost", self.config.port, reuse_port=True
        )

        # set path to the .rdb file in the config
        path = os.path.join(self.config.dir, self.config.dbfilename)
        self.config.db_path = path

        self.db.update_store(self.store, path)

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
            resp = RedisProtocolParser()

            byte_data = resp.decoder(data)
            logging.debug(f"bytes data is {byte_data}")
            result = await self.handle_command(byte_data)
            encoded = resp.encoder(result)
            logging.debug(f"ENCODED DATA : {encoded}")
            print(f"ENCODED DATA : {encoded}")
            if encoded:
                writer.write(encoded)
            else:
                # Send PONG response back to the client
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
            response = self.store.xadd(key, id, args)
            return response
        elif keyword == "XRANGE":
            key = args.pop(0)
            response = self.store.xrange(key, args)
            return response
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
            response = await self.store.xread(streams, id, block_ms)
            return response
        elif keyword == 'INFO':
            if args[0].lower() == 'replication':
                rep = self.config.replication.view_info()
                return rep
        else:
            return None

    @staticmethod
    def check_index(keyword, array):
        for i in range(len(array)):
            if keyword.lower() == array[i].lower():
                return i
        return None
