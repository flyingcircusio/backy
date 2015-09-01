import time
import os

# Internally we rely on time.time() as that is a lot more
# convenient to compute with for us.
# However, we need to stay consistent and this is an OKish
# workaround to keep everything in check.

os.environ['TZ'] = 'UTC'
time.tzset()
