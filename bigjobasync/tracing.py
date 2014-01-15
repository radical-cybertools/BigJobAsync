#!/usr/bin/env python

"""Execution profile tracing. 
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013, The RADICAL Project at Rutgers"
__license__   = "MIT"

from radical.utils import Singleton

class Traceable(object):

    def __init__(self):
        self.trace = []

    def set(self, data)):
        self.trace.append(data)


    def get_trace(self):
        return self.trace()
