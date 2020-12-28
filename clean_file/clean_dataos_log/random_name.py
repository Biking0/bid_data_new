import hashlib
import time

print hashlib.sha256(str(time.time())).hexdigest()[0:5]