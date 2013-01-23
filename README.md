you have lots of disks. all disks fail.
---------------------------------------

this set of scripts helps you monitor 1000's of disks and flags the ones that need attention. it won't stop disks failing, and won't stop you hating them, but will help you avoid losing data by detecting the nasty failing disks as quickly as possible. no doubt you will still go insane but d/dt of insanity rate will hopefully decrease.

**smartGather**

this script runs daily and gathers *smartctl -a* for all the disks down both ports to the drives. the putput is all gzip'd to reduce storage. it uses smartFormat.py and will email you about problematic disks.

**smartFormat.py**

this analyses daily (or more frequent) smartGather gzip'd output and prints out either the state of all disks, or only the most serious problems that need to be dealt with right now eg. Pending/Offline sectors or disks that are failing now. it handles seagate and hitachi, SATA and SAS.

**failureRate.py**

look at large amounts (eg. several years worth) of smartGather files and compute and graph the failure rates. it handles disks being moved around and being used as spares elsewhere. the script is smart enough to create and use databases of previously read smartGather files which speeds things up greatly.
when plotting, the format of the ''special'' file is eg.

    # check sweeps
    set label "scrub" at "2011-02-07-19:00:00",0.5 rotate
    ...
