# Molecular Dynamics Examples

This directory contains example BigJobAsync scripts for molecular dynamics workflows and applications. 

## 1. Free Energy Calculations (`freenrg`)

This example shows how to run a set of free energy calculations using AMBER / [MMPBSA.py](http://pubs.acs.org/doi/abs/10.1021/ct300418h).

### 1.1 Configuration

A simple configuration file (`config.py`)is provided in which the allocation and resource 
parameters are set. Change any of the values to your specific needs: 

```
CONFIG = {
    'resource'      : 'XSEDE.STAMPEDE',
    'username'      : 'tg802352',
    'workdir'       : '/scratch/00988/tg802352/freenrg/',
    'allocation'    : 'TG-MCB090174'
}
```

### 1.2 Test Mode

The `freenrg` script provides a 'test mode' in which only a single task is submitted to the remote cluster. This tasks checks wether the environment is healthy / usable and gathers some information about AMBER / MMPBSA. 

Before you start running large simulations on a resource, you should run test mode at least once to ensure that everything (?) is in place:

```
$> python freenrg.py --test
``` 

The output should look like this:

```
 * Task MMPBSA-test-task state changed from 'New' to 'TransferringInput'.
 * Task MMPBSA-test-task state changed from 'TransferringInput' to 'WaitingForExecution'.
 * Resource '<_BigJobWorker(_BigJobWorker-9, started daemon)>' state changed from 'New' to 'Pending'.
 * Task MMPBSA-test-task state changed from 'WaitingForExecution' to 'Pending'.
 * Resource '<_BigJobWorker(_BigJobWorker-9, started daemon)>' state changed from 'Pending' to 'Running'.
 * Task MMPBSA-test-task state changed from 'Pending' to 'Running'.
 * Task MMPBSA-test-task state changed from 'Running' to 'WaitingForOutputTransfer'.
 * Task MMPBSA-test-task state changed from 'WaitingForOutputTransfer' to 'TransferringOutput'.
 * Task MMPBSA-test-task state changed from 'TransferringOutput' to 'Done'.

Test task results:
MMPBSA path:/opt/apps/intel13/mvapich2_1_9/amber/12.0/bin/MMPBSA.py
MMPBSA version:MMPBSA.py: Version 13.0
```

