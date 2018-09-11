import time
from datetime import timedelta
from log_parser import LogParser

start_time = time.monotonic()

parser = LogParser('logs.txt')
parser.parse()

end_time = time.monotonic()
print('Total time:', timedelta(seconds=end_time - start_time))
