import os
import sys
import time
import signal
import threading
import atexit
import code
import Queue
import traceback
import logging

STACK_FILE = "/tmp/dump-stack-traces.txt"

_stack_interval = 10.0
_stack_running = False
_stack_queue = None
_stack_lock = None

def _stacktraces():
    code = ""
    for threadId, stack in sys._current_frames().items():
        code += "\n# ProcessId: %s" % os.getpid()
        code += "# ThreadID: %s\n" % threadId
        for filename, lineno, name, line in traceback.extract_stack(stack):
            code += "File: \"%s\", line %d, in %s\n" % (filename, lineno, name)
            if line:
                code += "  %s\n" % (line.strip())
    logging.error(code)

def _monitor():
    try:
        mtime = os.path.getmtime(STACK_FILE)
    except:
        mtime = None
    while 1:
        try:
            current = os.path.getmtime(STACK_FILE)
        except:
            current = None

        if current is not None and current != mtime:
            mtime = current
            _stacktraces()

        # Go to sleep for specified interval.
        try:
            return _stack_queue.get(timeout=_stack_interval)
        except:
            pass

def _exiting():
    global _stack_queue
    try:
        _stack_queue.put(True)
    except:
        pass
    _stack_thread.join()


def _start(interval=1.0):
    global _stack_interval
    if interval < _stack_interval:
        _stack_interval = interval

    global _stack_running
    global _stack_lock
    _stack_lock.acquire()
    if not _stack_running:
        prefix = "monitor (pid=%d):" % os.getpid()
        logging.info("%s Starting stack trace monitor." % prefix)
        _stack_running = True
        _stack_thread.start()
    _stack_lock.release()

def register_stack_listener():
    global _stack_queue
    global _stack_lock
    global _stack_thread
    _stack_queue = Queue.Queue()
    _stack_lock = threading.Lock()
    _stack_thread = threading.Thread(target=_monitor)
    _stack_thread.setDaemon(True)
    atexit.register(_exiting)
    _start()

