import asyncio
from dataclasses import dataclass, asdict, field


@dataclass
class Replica:
    host: str
    port: int
    reader: asyncio.StreamReader = None
    writer: asyncio.StreamWriter = None
    buffer_queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    is_ready: bool = True


@dataclass(kw_only=True)
class ReplicationConfig:

    role: str = "master"
    master_replid: str = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    master_repl_offset: int = 0
    command_offset: int = 0
    _slaves_list: list[Replica] = field(default_factory=list)
    _connected_slaves: int = 0

    def view_info(self) -> str:
        key_value_pairs = asdict(self)
        response = "\r\n".join(
            [f"{key}:{value}" for key, value in key_value_pairs.items()]
        )
        return response + "\r\n"

    @property
    def connected_slaves(self) -> int:
        return self._connected_slaves

    @connected_slaves.setter
    def connected_slaves(self, value: int) -> None:
        self._connected_slaves = value

    def add_slave(self, slave: Replica) -> None:
        self._slaves_list.append(slave)
        self.connected_slaves = len(self._slaves_list)

    def psync(self) -> str:
        cmd = f"FULLRESYNC {self.master_replid} {self.master_repl_offset}"
        return cmd

    def empty_rdb(self) -> bytes:
        empty_rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
        empty_rdb = bytes.fromhex(empty_rdb_hex)
        res = f"${len(empty_rdb)}\r\n".encode()
        return res + empty_rdb


@dataclass(kw_only=True)
class ServerConfiguration:

    port: int
    host: str = "127.0.0.1"
    dir: str
    dbfilename: str
    db_path: str = None

    replication: list[ReplicationConfig] = field(default_factory=ReplicationConfig)
    slave_tasks: list[asyncio.Task] = field(default_factory=list)

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
    rep = ReplicationConfig()
    rdb = rep.empty_rdb()
    print("RDB: ", rdb)
