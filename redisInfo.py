#coding=utf-8
import redis

COLOR_RED = "\033[31;49;1m %s \033[31;49;0m"

COLOR_GREED = "\033[32;49;1m %s \033[39;49;0m"

COLOR_YELLOW = "\033[33;49;1m %s \033[33;49;0m"

COLOR_BLUE = "\033[34;49;1m %s \033[34;49;0m"

COLOR_PINK = "\033[35;49;1m %s \033[35;49;0m"

COLOR_GREENBLUE = "\033[36;49;1m %s \033[36;49;0m"


def getHumanSize(value):
    gb = 1024 * 1024 * 1024.0
    mb = 1024 * 1024.0
    kb = 1024.0
    if value >= gb:
        return COLOR_RED % (str(round(value / gb, 2)) + " gb")
    elif value >= mb:
        return COLOR_YELLOW % (str(round(value / mb, 2)) + " mb")
    elif value >= kb:
        return COLOR_BLUE % (str(round(value / kb, 2)) + " kb")
    else:
        return COLOR_GREED % (str(value) + "b")


month = 3600 * 24 * 30
result = []
client = redis.Redis(host="xxx", port=xxx)
client.info()

count = 0
for key in client.keys('*'):
    try:
        count += 1
        idleTime = client.object('idletime', key)
        refcount = client.object('refcount', key)
        length = client.debug_object(key)['serializedlength']
        value = idleTime * refcount
        print "%s key :%s , idletime : %s,refcount :%s, length : %s , humSize  :%s" % (count, key, idleTime, refcount, length, getHumanSize(length))
    except Exception:
        pass