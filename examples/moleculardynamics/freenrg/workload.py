""" (Sample) workload definition file.
"""

WORKLOAD = []

# replace the paths with your input file paths on stampede

for tj in range(1, 51):

    task = {
        "runtime" : 60, # minutes per task
        "nmode"   : "/home1/00988/tg802352/MMPBSASampleDATA/nmode.5h.py",
        "com"     : "/home1/00988/tg802352/MMPBSASampleDATA/com.top.2",
        "rec"     : "/home1/00988/tg802352/MMPBSASampleDATA/rec.top.2",
        "lig"     : "/home1/00988/tg802352/MMPBSASampleDATA/lig.top",
        "traj"    : "/home1/00988/tg802352/MMPBSASampleDATA/trajectories/rep%s.traj" % tj
    }

    WORKLOAD.append(task)
