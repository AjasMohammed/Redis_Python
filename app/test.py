from utilities import CommandHandler, Store

store = Store()
cmd = CommandHandler()

ch = CommandHandler()
s = ch.call_cmd("set", key="name", value="hello", store=store)
g = ch.call_cmd("get", key="name", store=store)

print(s, g)
print(store)