import time
from .store import Store


class DatabaseParser:
    def __init__(self) -> None:
        self.key_value_pair: dict = {}

    def parse_lenght(self, data: bytes, current_index: int):

        byte = data[current_index]
        current_index += 1
        ms2_bits = (
            byte >> 6
        )  # Right Shift 6bits to bring the most significant 2 bits to the last
        length = None

        if ms2_bits == 0b00:
            length = byte

        elif ms2_bits == 0b01:
            first_byte = (
                byte & 0b0011_1111
            )  # Bitwise AND operation. Removing the Most Significant 2 bits in the front
            second_byte = data[current_index]
            current_index += 1
            # Shifting the 6bits to 8bits left to provide space for adding the second byte
            length = (first_byte << 8) | second_byte  # Bitwise OR operation

        elif ms2_bits == 0b10:
            length = int.from_bytes(
                data[current_index : current_index + 4]
            )  # Value of next 4bytes is length
            current_index += 4

        elif ms2_bits == 0b11:
            byte = byte & 0b00111111  # Remove the most significant 2 bits
            length = 2**byte

        return (length, current_index)

    def parse_rdb_string(self, data: bytes, current_index: int):
        length, current_index = self.parse_lenght(data, current_index)
        string = data[current_index : current_index + length]
        current_index += length

        return (string, current_index)

    def database_parser(self, path: str):
        try:
            with open(path, "rb") as file:
                data = file.read()
        except:
            return None

        magic, data = data[:5], data[5:]
        ver, data = data[:4], data[4:]
        current_index = 0
        expire_time = None

        while current_index < len(data):
            op_code = data[current_index]
            current_index += 1

            if op_code == 0xFA:
                key, current_index = self.parse_rdb_string(data, current_index)
                value, current_index = self.parse_rdb_string(data, current_index)

            elif op_code == 0xFE:
                db_number, current_index = self.parse_lenght(data, current_index)

            elif op_code == 0xFB:
                resizedb, current_index = self.parse_lenght(data, current_index)
                resizedb, current_index = self.parse_lenght(data, current_index)

            elif op_code == 0xFD:  # Expiry time in seconds
                expire_time = int.from_bytes(
                    data[current_index : current_index + 4], "little"
                )
                current_index += 4

            elif op_code == 0xFC:  # Expiry time in milliseconds
                expire_time = int.from_bytes(
                    data[current_index : current_index + 8], "little"
                )
                current_index += 8
                expire_time = expire_time / 1000

            elif op_code == 0xFF:
                break

            elif op_code == 0x00:
                key, current_index = self.parse_rdb_string(data, current_index)
                value, current_index = self.parse_rdb_string(data, current_index)

                if expire_time and expire_time > time.time():

                    self.key_value_pair[key.decode("utf-8")] = (
                        value.decode("utf-8"),
                        expire_time,
                    )

                expire_time = None
            else:
                continue

        return self.key_value_pair

    def update_store(self, store: Store, path: str):
        self.database_parser(path)
        store.store.update(self.key_value_pair)
