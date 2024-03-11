DELIMETER = "\r\n"

class RedisProtocolParser:
    def __init__(self):
        self.decoded = None
        self.encoded = None

    def encoder(self, data: bytes):
        self.encoded = None
        if isinstance(data, str):
            if data.isdigit():
                self.encoded = self.integer(data, True)
            else:
                self.encoded = self.bulk_string(data, encode=True)

        elif isinstance(data, list):
            self.encoded = self.array(data, True)

        return self.encoded

    def decoder(self, data: bytes):
        self.decoded = None
        data = data.decode()

        while DELIMETER in data:
            if data.startswith("+"):
                data, keyword = self.simple_string(data)
                self.join_data(keyword)

            elif data.startswith("-"):
                return self.simple_string(data)

            index = data.find(DELIMETER)

            if data.startswith("*"):
                self.decoded = []
                data = data[index + 2 :]
            elif data.startswith("$"):
                data, keyword = self.bulk_string(data, index)
                self.join_data(keyword)
            elif data.startswith(":"):
                num = int(data[1 : index + 2].rstrip(DELIMETER))
                data = data[index + 2 :]
                self.join_data(num)

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
    def simple_string(data, encode=False):
        if encode:
            return "+" + data + DELIMETER
        else:
            keyword = data[1:].rstrip(DELIMETER)
            data = data[1 + len(keyword) + 2 :]
            return data, keyword

    @staticmethod
    def simple_error(data, encode=False):
        return data[1:].rstrip(DELIMETER)

    @staticmethod
    def bulk_string(data, index=0, encode=False):
        if encode:
            raw_data = data.split(" ")
            convert_data = lambda keywords: "".join(
                f"${len(keyword)}{DELIMETER}{keyword}{DELIMETER}"
                for keyword in keywords
            )
            return convert_data(raw_data)
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
        if encode:
            prefix = "*" + str(len(data)) + DELIMETER
            resp = RedisProtocolParser()
            mapped_data = map(lambda keyword: resp.encoder(keyword), data)
            return prefix + "".join(mapped_data)
