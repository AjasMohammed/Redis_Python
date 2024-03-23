DELIMETER = "\r\n"


class RedisProtocolParser:
    STRING_CONSTANTS = {
        "pong",
        "ok",
        "string",
        "integer",
        "list",
        "hash",
        "stream",
        "none",
    }

    def __init__(self):
        self.decoded = None
        self.encoded = None

    def encoder(self, data: list | str | dict) -> bytes | None:
        self.encoded = None
        if isinstance(data, str):
            # if data.isdigit():
            #     self.encoded = self.integer(data, encode=True)
            # else:
            if data.lower() in self.STRING_CONSTANTS:
                self.encoded = self.simple_string(data, encode=True)
            else:
                self.encoded = self.bulk_string(data, encode=True)

        elif isinstance(data, list):
            self.encoded = self.array(data, encode=True)

        elif isinstance(data, dict):
            self.encoded = self.simple_error(data["error"], encode=True)

        else:
            self.encoded = "$-1\r\n"  # Null Bulk String
        return bytes(self.encoded, "utf-8")

    def decoder(self, data: bytes):
        self.decoded = None
        try :
            data = data.decode()
        except UnicodeDecodeError:
            print('Unicode Error')

        while DELIMETER in data:
            if data.startswith("+"):
                data, keyword = self.simple_string(data)
                if isinstance(data, list):
                    data = [keyword, *data]
                    return data
                else:
                    self.join_data(keyword)

            elif data.startswith("-"):
                return self.simple_error(data)

            index = data.find(DELIMETER)

            if data.startswith("*"):
                if data.count("*") > 1 and "*\r" not in data:
                    self.decoded, data = self.array(data)
                else:
                    self.decoded = []
                    data = data[index + 2 :]
            elif data.startswith("$"):
                data, keyword = self.bulk_string(data, index)
                self.join_data(keyword)
            elif data.startswith(":"):
                num = int(data[1 : index + 2].rstrip(DELIMETER))
                data = data[index + 2 :]
                self.join_data(num)
            else:
                break
        return self.decoded

    def join_data(self, data):
        if self.decoded == None:
            self.decoded = str(data)
        else:
            if isinstance(self.decoded, list):
                self.decoded.append(str(data))
            else:
                self.decoded += str(data)

    @staticmethod
    def simple_string(data: str, encode=False):
        if encode:
            return "+" + data + DELIMETER
        else:
            if ' ' not in data:
                keyword = data[1:].rstrip(DELIMETER)
                data = data[1 + len(keyword) + 2 :]
            else:
                data = data[1:].rstrip(DELIMETER).split(' ')
                keyword = data[0]
            return data[1:], keyword

    @staticmethod
    def simple_error(data, encode=False):
        if encode:
            return "-ERR " + data.rstrip(DELIMETER) + DELIMETER
        else:
            return data[1:].rstrip(DELIMETER)

    @staticmethod
    def bulk_string(data, index=0, encode=False):
        if encode:
            raw_data = data.split(" ")
            convert_data = lambda keywords: "".join(
                f"${len(keyword)}{DELIMETER}{keyword}{DELIMETER}"
                for keyword in keywords
            )
            new_data = convert_data(raw_data)
            return new_data
        else:
            start_index = index + 2
            string_length = int(data[1:start_index])
            end_index = start_index + string_length + 2
            keyword = data[start_index:end_index].rstrip(DELIMETER)
            data = data[end_index:]
            return data, keyword

    @staticmethod
    def integer(data, encode=False):
        if encode:
            return ":" + str(data) + DELIMETER

    @staticmethod
    def array(data, encode=False):
        resp = RedisProtocolParser()
        if encode:
            prefix = "*" + str(len(data)) + DELIMETER
            mapped_data = map(
                lambda keyword: resp.encoder(keyword).decode("utf-8"), data
            )

            return prefix + "".join(mapped_data)
        else:
            new_data = []
            index_1 = 0
            x = 0
            while "*" in data:
                index_2 = data[index_1 + 1 :].find("*") + 1
                if index_2 == 0:
                    index_2 = -1
                d = resp.decoder(data[index_1:index_2].encode("utf-8"))
                new_data.append(d)
                data = data[index_2:]

            return new_data, data


# Usage example
if __name__ == "__main__":
    parser = RedisProtocolParser()
    decoded_data = parser.decoder(
        "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\n123\r\n*3\r\n$3\r\nSET\r\n$3\r\nbar\r\n$3\r\n456\r\n*3\r\n$3\r\nSET\r\n$3\r\nbaz\r\n$3\r\n789\r\n".encode(
            "utf-8"
        )
    )
    print(decoded_data)
