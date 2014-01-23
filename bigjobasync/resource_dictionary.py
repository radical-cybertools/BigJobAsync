#!/usr/bin/env python

"""Various compute resources are defined here.
"""

__author__    = "Ole Weidner"
__email__     = "ole.weidner@rutgers.edu"
__copyright__ = "Copyright 2013-2014, The RADICAL Project at Rutgers"
__license__   = "MIT"

# ----------------------------------------------------------------------------
# RESOURCE DICTIONARY

RESOURCES = {
    'XSEDE.STAMPEDE': {
        'redis_host'      : 'gw68.quarry.iu.teragrid.org:6379',
        'redis_pwd'       : 'ILikeBigJob_wITH-REdIS',
        'jobmgr_url'      : 'slurm+ssh://stampede.tacc.utexas.edu',
        'jobmgr_queue'    : 'normal',
        'core_increment'  : 16, # allocations need to be multiples of this
        'shared_fs_url'   : 'sftp://stampede.tacc.utexas.edu/',
    },
    
    'XSEDE.LONESTAR': {
        'redis_host'    : 'gw68.quarry.iu.teragrid.org:6379',
        'redis_pwd'     : 'ILikeBigJob_wITH-REdIS',
        'jobmgr_url'    : 'sge+ssh://lonestar.tacc.utexas.edu',
        'jobmgr_queue'  : 'normal',
        'shared_fs_url' : 'sftp://lonestar.tacc.utexas.edu/',
        'spmd_variation': '12way'
    },

    'FUTUREGRID.ALAMO': {
        'redis_host'    : 'gw68.quarry.iu.teragrid.org:6379',
        'redis_pwd'     : 'ILikeBigJob_wITH-REdIS',
        'jobmgr_url'    : 'pbs+ssh://alamo.futuregrid.org',
        'jobmgr_queue'  : 'short',
        'shared_fs_url' : 'sftp://alamo.futuregrid.org/',
    },

    'FUTUREGRID.INDIA': {
        'redis_host'    : 'gw68.quarry.iu.teragrid.org:6379',
        'redis_pwd'     : 'ILikeBigJob_wITH-REdIS',
        'jobmgr_url'    : 'pbs+ssh://india.futuregrid.org',
        'jobmgr_queue'  : 'batch',
        'shared_fs_url' : 'sftp://india.futuregrid.org/',
    },

    'FUTUREGRID.SIERRA': {
        'redis_host'    : 'gw68.quarry.iu.teragrid.org:6379',
        'redis_pwd'     : 'ILikeBigJob_wITH-REdIS',
        'jobmgr_url'    : 'pbs+ssh://sierra.futuregrid.org',
        'jobmgr_queue'  : 'batch',
        'shared_fs_url' : 'sftp://sirra.futuregrid.org/',
    }
}
    
