import time

count = 0
try:
    count += 1
    raise ValueError("klajsdf")
    count += 1
except:
    pass
print(count)