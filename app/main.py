import asyncio
import logging
from .utilities import RedisProtocolParser, Store


logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
)


# Define coroutine to handle client connections
async def handle_client(reader, writer, store):
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

        result = await handle_command(byte_data, store)

        encoded = resp.encoder(result)
        if encoded:
            writer.write(encoded)
        else:
            # Send PONG response back to the client
            writer.write(pong)
        await writer.drain()
    # Close the connection
    writer.close()


async def handle_command(data, store):
    logging.debug(f"data is {data}")
    keyword, *args = data
    keyword = keyword.upper()
    if keyword == "PING":
        return "PONG"
    elif keyword == "ECHO":
        return " ".join(args)
    elif keyword == "SET":
        store.set(args[0], args[1])
        return "OK"
    elif keyword == "GET":
        data = store.get(args[0])
        return data
    else:
        return None


async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    store = Store()
    # Start the server
    server = await asyncio.start_server(
        lambda r, w: handle_client(r, w, store), "localhost", 6379, reuse_port=True
    )

    # Serve clients indefinitely
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
