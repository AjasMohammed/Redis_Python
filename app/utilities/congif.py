from dataclasses import dataclass, asdict, field


@dataclass(kw_only=True)
class Replication:

    role: str = "master"
    master_replid: str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    master_repl_offset: int = 0

    def view_info(self):
        key_value_pairs = asdict(self)
        response = "\r\n".join(
            [f"{key}:{value}" for key, value in key_value_pairs.items()]
        )

        return response

    def psync(self):
        cmd = f"FULLRESYNC {self.master_replid} {self.master_repl_offset}"
        return cmd
    
    def empty_rdb(self):
        empty_rdb_hex = '524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2'
        empty_rdb = bytes.fromhex(empty_rdb_hex)
        res = f"${len(empty_rdb)}\r\n".encode()
        return res + empty_rdb


@dataclass(kw_only=True)
class ServerConfiguration:

    port: int
    host: str = "localhost"
    dir: str
    dbfilename: str
    db_path: str = None

    replication: list = field(default_factory=Replication)

    def handle_config(self, new_conf: list):
        keyword = new_conf.pop(0)
        if keyword.upper() == "GET":
            return self.get_config(new_conf[0])
        elif keyword.upper() == "SET":
            self.set_config(new_conf[0], new_conf[1])
            return True

    def get_config(self, key: str):
        value = getattr(self, key)
        return [key, value]

    def set_config(self, key: str, value: str | int):
        setattr(self, key, value)


if __name__ == "__main__":
    config = ServerConfiguration()
    print(config.get_config("port"))
    config.set_config("port", 8000)
    print(config.get_config("port"))
