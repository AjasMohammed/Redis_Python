# Uncomment this to pass the first stage
import socket


def main():
    # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")

    pong = b"+PONG\r\n"

    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)

    connection, address = server_socket.accept() # wait for client
    with connection:
        while True:
            data = connection.recv(1024).decode("utf-8")
            if not data:  # Handles multiple PINGS from the same connection
                break
            connection.sendall(pong)


if __name__ == "__main__":
    main()
