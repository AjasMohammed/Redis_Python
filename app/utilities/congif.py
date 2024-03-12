from dataclasses import dataclass


@dataclass(kw_only=True)
class ServerConfiguration:

    port: int = 6379
    dir: str
    dbfilename: str

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
