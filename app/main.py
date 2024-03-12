import asyncio
from .utilities import ServerConfiguration
import argparse
from .server import Server




async def main():
    parser = argparse.ArgumentParser(description="Redis server")
    parser.add_argument(
        "--dir",
        help="Directory to store data",
    )
    parser.add_argument(
        "--dbfilename",
        help="Filename to store data",
    )
    args = parser.parse_args()  # parse commandline arguments

    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    config = ServerConfiguration(dir=args.dir, dbfilename=args.dbfilename)

    # Start the server
    server = Server(config)
    await server.start_server()


if __name__ == "__main__":
    asyncio.run(main())
