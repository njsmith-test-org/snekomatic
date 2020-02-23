import sys
import trio

from .app import worker

sys.stdout.reconfigure(line_buffering=True)

trio.run(worker, sys.argv[1])
