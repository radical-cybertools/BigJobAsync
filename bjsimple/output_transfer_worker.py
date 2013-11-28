#!/usr/bin/env python

"""DOCSTRING
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

import saga
import time
import Queue
import constants
import multiprocessing

# ----------------------------------------------------------------------------
#
class _OutputTransferWorker(multiprocessing.Process):

    def __init__(self, ready_to_transfer_output_q, done_q, failed_q):
        multiprocessing.Process.__init__(self)
        self.daemon = True
        self._stop  = False

        # All queue an OutputFileTransferWorker can access
        self._tasks_done_q = done_q
        self._tasks_failed_q = failed_q
        self._tasks_ready_to_transfer_output_q = ready_to_transfer_output_q

    def stop(self):
        """DS
        """
        self._stop = True

    def run(self):
        """DS
        """
        while self._stop is False:
            while True:
                try:
                    task = self._tasks_ready_to_transfer_output_q.get_nowait()
                    print "%s: OutputFileTransferWorker. NOT IMPLEMENTED YET" % task
                    self._tasks_done_q.put(task)
                except Queue.Empty:
                    break

            time.sleep(1)