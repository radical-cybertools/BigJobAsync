# Molecular Dynamics Examples

This directory contains example BigJobAsync scripts for molecular dynamics workflows and applications. 

## 1. Free Energy Calculations (`freenrg`)

This example shows how to run a set of free energy calculations using AMBER / [MMPBSA.py](http://pubs.acs.org/doi/abs/10.1021/ct300418h).

### Prerequisites 

Make sure Amber works / is available on the target machine. For example on Stampede:

```
$> module load amber

$> /opt/apps/intel13/mvapich2_1_9/amber/12.0/bin/MMPBSA.py --version
MMPBSA.py: Version 13.0
```


```
{
    PreExec:    "module load amber"
    MMPBSATool: "MMPBSA.py"
}

{
    replica:    1,
    topfiles:   ["com.top.2", "rec.top.2", "lig.top"],
    trajectory: rep1.traj,
    nmode:      nmode.py
} 
```