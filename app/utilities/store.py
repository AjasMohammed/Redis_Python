class Store:

    def __init__(self):
        self.store = {}

    def set(self, key, value):
        self.store[key] = value
        return True

    def get(self, key):
        value = self.store.get(key, None)
        return value
