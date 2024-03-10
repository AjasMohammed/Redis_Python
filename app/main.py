import asyncio


# Define coroutine to handle client connections
async def handle_client(reader, writer):
    pong = b"+PONG\r\n"
    while True:
        # Read data from the client
        data = await reader.read(1024)
        if not data:
            break
        # Send PONG response back to the client
        writer.write(pong)
        await writer.drain()
    # Close the connection
    writer.close()


async def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Start the server
    server = await asyncio.start_server(handle_client, "localhost", 6379, reuse_port=True)

    # Serve clients indefinitely
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
