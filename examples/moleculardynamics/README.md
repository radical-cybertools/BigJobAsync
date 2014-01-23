# Molecular Dynamics Examples

This directory contains example BigJobAsync scripts for molecular dynamics workflows and applications. 

## 1. Free Energy Calculations (`freenrg`)

This example shows how to run a set of free energy calculations using AMBER / [MMPBSA.py](http://pubs.acs.org/doi/abs/10.1021/ct300418h).

### 1.1 Configuration

A simple configuration file (`config.py`)is provided in which the allocation and resource 
parameters are set. Configuration files are passed to the `freenrg.py` tool via the `--config=` flag. Change any of the values in the file to your specific needs: 

```
CONFIG = {
    'resource'      : 'XSEDE.STAMPEDE',
    'username'      : 'tg802352',
    'workdir'       : '/scratch/00988/tg802352/freenrg/',
    'allocation'    : 'TG-MCB090174'
}
```

### 1.2 Test Mode

The `freenrg` script provides two 'test modes' (`--checkenv` and `--testjob`) in which only a single task is submitted to the remote cluster to check wether the environment is healthy and usable.  

Before you start running large simulations on a resource, you should run `--checkenv` test mode at least once to ensure that the environment is ok:

```
$> python freenrg.py --config=config.py --checkenv
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

Once `--checkenv` has passed, you can run `--testjob`. In this test mode, a single free energy calculation job is launched together with some [sample input data](http://google.com) that is downloaded and transferred on the fly.  


```
$> python freenrg.py --config=config.py --testjob
```

The output should look like this:

```
 * Task MMPBSA-fe-test-task state changed from 'New' to 'TransferringInput'.
 * Task MMPBSA-fe-test-task state changed from 'TransferringInput' to 'WaitingForExecution'.
 * Resource '<_BigJobWorker(_BigJobWorker-9, started daemon)>' state changed from 'New' to 'Pending'.
 * Task MMPBSA-fe-test-task state changed from 'WaitingForExecution' to 'Pending'.
 * Resource '<_BigJobWorker(_BigJobWorker-9, started daemon)>' state changed from 'Pending' to 'Running'.
 * Task MMPBSA-fe-test-task state changed from 'Pending' to 'Running'.
 * Task MMPBSA-fe-test-task state changed from 'Running' to 'WaitingForOutputTransfer'.
 * Task MMPBSA-fe-test-task state changed from 'WaitingForOutputTransfer' to 'TransferringOutput'.
 * Task MMPBSA-fe-test-task state changed from 'TransferringOutput' to 'Done'.

Test task results:
Loading and checking parameter files for compatibility...
Preparing trajectories for simulation...
20 frames were processed by cpptraj for use in calculation.

Running calculations on normal system...

Beginning GB calculations with /opt/apps/intel13/mvapich2_1_9/amber/12.0/bin/mmpbsa_py_energy
  calculating complex contribution...
  calculating receptor contribution...
  calculating ligand contribution...

Beginning PB calculations with /opt/apps/intel13/mvapich2_1_9/amber/12.0/bin/mmpbsa_py_energy
  calculating complex contribution...
  calculating receptor contribution...
  calculating ligand contribution...

Timing:
Total setup time:                           0.042 min.
Creating trajectories with cpptraj:         0.030 min.
Total calculation time:                     8.192 min.

Total GB calculation time:                  1.250 min.
Total PB calculation time:                  6.942 min.

Statistics calculation & output writing:    0.000 min.
Total time taken:                           8.274 min.


MMPBSA.py Finished! Thank you for using. Please cite us if you publish this work with this paper:
   Miller III, B. R., McGee Jr., T. D., Swails, J. M. Homeyer, N. Gohlke, H. and Roitberg, A. E.
   J. Chem. Theory Comput., 2012, 8 (9) pp 3314--3321
mmpbsa_py_energy found! Using /opt/apps/intel13/mvapich2_1_9/amber/12.0/bin/mmpbsa_py_energy
cpptraj found! Using /opt/apps/intel13/mvapich2_1_9/amber/12.0/bin/cpptraj
```

### 1.3 Running a Free Energy Calculation Workload

A sample workload file (workload.py) is provided in which multiple MMPBSA tasks are defined. A workload is passed to the freenrg.py tool via the --workload= flag. Change the workload in the file to your specific needs:

```
WORKLOAD = []

for tj in range(1, 2):

    task = {
        "runtime" : 60, # minutes per task
        "nmode"   : "/home1/00988/tg802352/MMPBSASampleDATA/nmode.5h.py",
        "com"     : "/home1/00988/tg802352/MMPBSASampleDATA/com.top.2",
        "rec"     : "/home1/00988/tg802352/MMPBSASampleDATA/rec.top.2",
        "lig"     : "/home1/00988/tg802352/MMPBSASampleDATA/lig.top",
        "traj"    : "/home1/00988/tg802352/MMPBSASampleDATA/trajectories/rep%s.traj" % tj,
    }

    WORKLOAD.append(task)
```

Once you have defined your workload you can execute it. Due to the potentially long runtime of your workload, it is highly advisable to run the script within a terminal multiplexer, like [tmux](http://robots.thoughtbot.com/a-tmux-crash-course). This allows you to _detach_ from your running script, log out from the lab machine and re-attach to it at a later point in time to check its progress.

> TIP: To start a new tmux session, type
> 
>     tmux
>    
> You can *detach* from a running tmux session by pressing `Ctrl-B D` and *re-attach* by launching tmux via `tmux attach`.

In your (new) `tmux` session, active your virtual environment, update BigJobAsync (just in case) and launch your workload:

```
source $HOME/MDStack/bin/activate
pip install bigjob
pip install --upgrade -e git://github.com/oleweidner/BigJobAsync.git@master#egg=bigjobasync

python freenrg.py --config=config.py --workload=workload.py
```
 

