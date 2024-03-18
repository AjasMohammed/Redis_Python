from dataclasses import dataclass, asdict, field


@dataclass(kw_only=True)
class Replication:

    role: str = "master"

    def view_info(self):
        key_value_pairs = asdict(self)
        response = " ".join(
            [f"{key}:{value}" for key, value in key_value_pairs.items()]
        )
        return response


@dataclass(kw_only=True)
class ServerConfiguration:

    port: int
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
