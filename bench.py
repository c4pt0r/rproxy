import redis

s = redis.Redis("127.0.0.1", 9037)

for i in range(10000):
    s.set(str(i), "A" * 1024)
