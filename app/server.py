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
        # Serve clients indefinitely
        async with server:
            await server.serve_forever()

    # Define coroutine to handle client connections
    async def handle_client(self, reader, writer):
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
            path = os.path.join(self.config.dir, self.config.dbfilename)
            keys = self.db.database_parser(path)
            return keys
        else:
            return None
