import time


class Store:

    def __init__(self):
        self.store = {}

        self.arguments = {
            "px": self.px,
            "ex": self.ex,
        }

    def set(self, key: str, value: any, args: list):
        print(key, value, args)
        """
        Set a key-value pair in the store with optional arguments for expiration time.

        :param key: The key to be set in the store.
        :param value: The value to be associated with the key.
        :param args: Optional arguments for expiration time.
        :return: True if the key-value pair is successfully set, False otherwise.
        """
        expire_time = None
        if args:
            args = list(map(str.lower, args))
            if "nx" in args:
                args.remove("nx")
                val = self.check_availability(key)
                if val:
                    return False
            elif "xx" in args:
                args.remove("xx")
                val = self.check_availability(key)
                if not val:
                    return False

            while len(args) > 0:
                arg = args[0]
                param = int(args[1])
                expire_time = self.call_args(arg, param)
                args = args[2:]

        self.store[key] = (value, expire_time)
        return True

    def get(self, key: str):
        """
        Retrieves the value associated with the given key from the store. If the key does not exist or the associated value has expired, returns None.
        Args:
            key: The key to retrieve the value for.
        Returns:
            The value associated with the given key, or None if the key does not exist or the associated value has expired.
        """
        value, expire_time = self.store.get(key, (None, None))
        if expire_time is not None and expire_time < time.time():
            del self.store[key]
            return None
        return value

    def call_args(self, arg: str, param: int):
        """
        A function to call the specified argument with the given parameter.
        It retrieves the function associated with the specified argument key,
        and if found, it calls the function with the provided parameter and returns the result.
        Parameters:
            arg: The argument key to look up the associated function.
            param: The parameter to be passed to the associated function.
        Returns:
            The result of calling the associated function with the provided parameter.
        """
        func = self.arguments.get(arg.lower())
        if func:
            return func(param)

    def check_availability(self, key: str):
        """
        Check the availability of a key in the store and return True if it is available,
        False otherwise.
        :param key: The key to check availability for.
        :return: True if the key is available, False otherwise.
        """
        if key in self.store:
            expire_time = self.store[key][1]
            if expire_time is not None and expire_time < time.time():
                del self.store[key]
                return False
            return True

    @staticmethod
    def px(expire_time: int):  # Expire time in milliseconds
        current_time = time.time() * 1000
        expiration_time = (
            current_time + expire_time
        ) // 1000  # converts milliseconds to seconds
        return expiration_time

    @staticmethod
    def ex(expire_time: int):  # Expire time in seconds
        current_time = time.time()
        return current_time + expire_time


if __name__ == "__main__":
    s = Store()
    s.set("hello", "world", ["PX", "100", "NX"])
    s.set("hey", "HEY", ["NX"])
    print(s.get("hello"))
    time.sleep(1)
    print(s.get("hey"))
