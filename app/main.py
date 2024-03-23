import asyncio
from .utilities import ServerConfiguration
import argparse
from .server import Server


async def main():
    parser = argparse.ArgumentParser(description="Redis server")
    parser.add_argument(
        "--port",
        default="6379",
        help="Port to listen on",
    )
    parser.add_argument(
        "--dir",
        default="/tmp/redis-files",
        help="Directory to store data",
    )
    parser.add_argument(
        "--dbfilename",
        default="dump.rdb",
        help="Filename to store data",
    )
    parser.add_argument(
        "--replicaof",
        default=[],
        nargs=2,
        help="Replica server",
    )
    args = parser.parse_args()  # parse commandline arguments

    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    config = ServerConfiguration(
        dir=args.dir, dbfilename=args.dbfilename, port=args.port
    )
    if args.replicaof:
        config.replication.role = "slave"
        host = args.replicaof[0]
        if host == "localhost":
            host = "127.0.0.1"
        config.replication.master_host = host
        config.replication.master_port = args.replicaof[1]

    # Start the server
    server = Server(config)
    await server.start_server()


if __name__ == "__main__":
    asyncio.run(main())

