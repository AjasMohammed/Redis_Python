import asyncio
import logging
from .redis_parser.resp import RedisProtocolParser


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Define coroutine to handle client connections
async def handle_client(reader, writer):
    pong = b"+PONG\r\n"
    while True:
        # Read data from the client
        data = await reader.read(1024)
        logging.debug(f"Recived data: {data}")
        if not data:
            break
        resp = RedisProtocolParser()
        byte_data = resp.decoder(data)
        logging.debug(f'bytes data is {byte_data}')

        result = await handle_command(byte_data)
        encoded = resp.encoder(result)
        if result:
            writer.write(bytes(encoded, "utf-8"))
        else:
            # Send PONG response back to the client
            writer.write(pong)
        await writer.drain()
    # Close the connection
    writer.close()


async def handle_command(data):
    logging.debug(f'data is {data}')
    keyword, *args = data
    keyword = keyword.upper()
    if keyword == "PING":
        return b"+PONG\r\n"
    elif keyword == "ECHO":
        return " ".join(args)
    else:
        return None


async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Start the server
    server = await asyncio.start_server(
        handle_client, "localhost", 6379, reuse_port=True
    )

    # Serve clients indefinitely
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
