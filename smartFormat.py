#!/usr/bin/env python

# (c) Robin Humble 2011-2013
# licensed under the GPLv3 or later

# read smartGather files (basically lot of smartctl -a's) from stdin, pretty format, print the important stuff...

import sys, string, re, socket

doGopher4 = 0
quiet = 0

fcMode=0
if len(sys.argv) > 1 and sys.argv[1] == "--fc":
   fcMode=1
   del sys.argv[1]

name = socket.gethostname()

if name == 'vu-pbs':
   if fcMode:
      expectDisks=16
      expectHosts=56
   else:
      #expectDisks={ 'default':96, 'hamster23':120, 'hamster24':120 }   # for when we have a tray extra of hitachis on port 3 doing mirroring
      expectDisks={ 'default':96 }
      expectHosts=30

   # dodgy disks that we permit...
   #  these SAS disks have had 1 write error, or read errors
   exceptions = { '0920B1EWV7':['write:.* 1$'],
                  '0920B13TSG':['write:.* 1$'],
                  '0920B1CGAC':['write:.* 1$'],
                  '0920B1ENP9':['write:.* 1$'],
                  '0920B13TS7':['write:.* 2$'] }  # was write: 1 for this disk

# no longer tracking read fails ->
#                  '0927B1PYXX':['read:.* 58$'],   # was read 31, now 33 - Wed Jan  2 15:10:05 EST 2013, now 58 - Mon Jan 21 14:52:11 EST 2013

   hostDisks = { 'hamster':96, 'marmot':96 }
   serialOffset=7

elif name == 'xepbs':
   expectDisks=24
   expectHosts=10
   exceptions = { '3KP0K3T8000085012DE9':['Elements in grown defect list: 2', 'read:.* 742$'],
                  '3HW30FKB00007539G4ET':['read:.* 5$', 'write:.* 8$'] }
   hostDisks = { 'xemds':32, 'sox':24 }
   serialOffset=6

else:
   print 'not xepbs, vu-pbs', name
   sys.exit(1)

if len(sys.argv) > 1:
   # in quiet mode we only print serious problems...
   quiet = 1

if not quiet:
   print 'verbose mode'

f = sys.stdin
rl = f.readlines()
f.close()

# run a ~grep

# sata
#egrep '/dev/|erial|ATA Error|Pending|Reallocated|Offline_Unc|In_the_past|FAILING_NOW' smart.2009-12-07-13:03:26 | grep -v '   0$'
r1 = re.compile( '/dev/|erial|ATA Error|Pending|Reported_Uncorrect|Reallocated|Offline_Unc|In_the_past|FAILING_NOW|SMART Disabled|Smartctl: Device Read Identity Failed' )
r2 = re.compile( '   0$' )
rr = []
for l in rl:
   if r1.search(l) and not r2.search(l):
      rr.append(l)
parsedLines = rr

if not quiet:
   print 'beta SAS support'

# sas
#egrep '/dev/|erial|SMART Health Status' $file | grep -v ' OK$' | egrep -B2 'SMART Health Status' >> $file.summary
r1 = re.compile( '/dev/|erial|SMART Health Status|^read:|^write:|^# 1  Background short  Failed in segment|^# 1  Background long   Failed in segment|Elements in grown defect list:' )
r2 = re.compile( ' OK$|  0$|defect list: 0$' )
rr = []
for l in rl:
   if r1.search(l) and not r2.search(l):
      rr.append(l)
parsedLines.extend( rr )

rl = parsedLines

# combined sata and sas
#rSerious = re.compile( 'Pending|Offline_Unc|In_the_past|FAILING_NOW|SMART Disabled|Smartctl: Device Read Identity Failed|\> # 1  Background short  Failed in segment|\> # 1  Background long   Failed in segment|\> read:.*[^( 0$)]|\> write:.*[^( 0$)]' )
#  - sas read fails aren't all that serious - should be remapped etc. -  Mon Jan 21 14:55:52 EST 2013
rSerious = re.compile( 'Pending|Offline_Unc|In_the_past|FAILING_NOW|SMART Disabled|Smartctl: Device Read Identity Failed|\> # 1  Background short  Failed in segment|\> # 1  Background long   Failed in segment|\> write:.*[^( 0$)]' )


#for l in rl:
#    print l.strip()
#sys.exit(1)

head=0
prefix=""
prevPrefix=""
faults={}
devs={}
for l in rl:
   # 'head' is the first 2 lines - the hamster port etc., and the serial number
   #print 'l', l.strip(), 'head', head
   if head:
      if l[:6] == "Serial":
         head=0
         l = l.split()[2]
         #print 'prefix', prefix, 'l', l
         key = ( prefix.split()[0], l )   # ( hamster23, 9QJ4M8ZL )
         devs[key] = prefix + l
         prefix += l.strip() + ' '
      else:
         print '*** ERROR *** found no serial number for', prefix
         checkNext = l.split()
         if len(checkNext) > 1 and len(checkNext[1]) > 4 and checkNext[1][:4] == '/dev':
            # found another 'head', but we're already in head mode...
            prefix = l.strip() + ' '
         else:
            head=0

   else:
      checkNext = l.split()
      if l[:2] == "--":   # handle old grep'd output
         continue
      if len(checkNext) > 0 and checkNext[0] == "argh":   # sasDisksOnPort failed. skip. will be flagged later.
         continue
      elif len(checkNext) > 1 and len(checkNext[1]) > 4 and checkNext[1][:4] == '/dev':  # found next head
         if head:
            print 'parsing error - head already 1'
            sys.exit(1)
         head=1
         prefix=l.strip() + ' '
         #print 'head=1', prefix
      else:
         # read the errors from each disk
         if prefix == prevPrefix:
            #print ' '*len(prefix) + ' \\-> ' + l.strip()
            if prevKey[1] in exceptions.keys():
               skippage = 0
               if quiet:  # only skip known bad disks in quiet mode to avoid spurious emails
                  for reg in exceptions[prevKey[1]]:
                     r = re.compile(reg) 
                     if r.search(l):
                        #print 'exception hit', prevKey, l
                        skippage = 1
               if skippage:
                  continue
            faults[prevKey].append( ' '*len(prefix) + ' \\-> ' + l.strip() )
         else:
            #print prefix + ' --> ' + l.strip()
            # hamster23 /dev/sdh /dev/sg66 port 0 disk 5 9QJ4M8ZL
            key = ( prefix.split()[0], prefix.split()[serialOffset] )   # ( hamster23, 9QJ4M8ZL )
            if key in faults.keys():
               hName = key[0].rstrip(string.digits)
               if hName != "v":  # compute node jbods are multipath, single host so errors can be dup'd
                  print 'parsing error - key', key, 'already in faults'
                  sys.exit(1)
            if key[1] in exceptions.keys():
               skippage = 0
               for reg in exceptions[key[1]]:
                  r = re.compile(reg)
                  if r.search(l):
                     #print 'exception 2 hit', key, l
                     skippage = 1
               if skippage:
                  continue
            faults[key] = [ prefix + ' --> ' + l.strip() ]
         prevPrefix = prefix
         prevKey = key

#print faults
#print devs

def getPartner(name):
   # look for twin
   hNum = int(h.lstrip(string.ascii_letters))
   hName = h.rstrip(string.digits)

   if hName not in hostDisks.keys():
      return None

   if hNum%2:
      hNum += 1
   else:
      hNum -= 1
   partner = hName + '%d' % hNum
   #print 'host', h, 'partner', partner
   return partner

count={}

for h, s in devs.keys():
   partner = getPartner(h)

   # count
   if h not in count.keys():
      count[h] = 0
   count[h] += 1

   if partner == None:
      continue

   # check we have it twice
   if ( partner, s ) not in devs.keys():
      print '*** ERROR ***', devs[(h,s)], 'has partner', partner,'that cannot see disk', s
      continue

   # check device names and numbers are right
   d1 = devs[(h,s)]
   d2 = devs[(partner,s)]
   if name == 'vu-pbs':
      if d1.split()[1:] != d2.split()[1:]:
         if d1.split()[1] != d2.split()[1] or d1.split()[3:] != d2.split()[3:]:  # ignore the sg entry (field 2) as that can be different and doesn't actually matter
            print ' *** ERROR *** different name/port for same disk'
            print 'd1', d1
            print 'd2', d2
         #else:
         #   print ' *** Warning *** different sg for same disk'
         #   print 'd1', d1
         #   print 'd2', d2


if len(count.keys()) != expectHosts + doGopher4:
   g = ''
   if doGopher4:
      g = ', gopher4'
   print '*** ERROR *** %d hamsters, marmots%s did not respond. only %d' % ( expectHosts+doGopher4, g, len(count.keys()) )

for h in count.keys():
   if name == 'vu-pbs':
      if isinstance(expectDisks, dict):
         if h in expectDisks.keys():
            ed = expectDisks[h]
         else:
            ed = expectDisks['default']
      else:
         ed = expectDisks
      if h != 'gopher4' and count[h] != ed:
         print '*** ERROR *** expectDisks', ed, 'on', h, 'but found', count[h]
      elif h == 'gopher4' and count[h] != 120:
         print '*** ERROR *** not 120 disks on', h, 'only', count[h]
   elif name == 'xepbs':
      hName = h.rstrip(string.digits)
      if hName == 'sox' and count[h] != 24:
         print '*** ERROR *** not 24 disks on', h, 'only', count[h]
      elif hName == 'xemds' and count[h] != 32:
         print '*** ERROR *** not 32 disks on', h, 'only', count[h]

def cmp(a,b):
   a,s = a
   b,s = b
   c = int(a.lstrip(string.ascii_letters)) - int(b.lstrip(string.ascii_letters))
   return c

printed=[]
f = faults.keys()
f.sort(cmp)
#print f

for h, s in f:
   partner = getPartner(h)

   # check we have it twice
   if ( partner, s ) not in faults.keys():
      d1 = faults[(h,s)]
      if partner != None:
         d1[0] += ' *** ERROR *** partner ' + partner + ' cannot see this disk'
      for d in d1:
         print d
      continue

   # check it's the same errors
   d1 = faults[(h,s)]
   if name == 'vu-pbs':
      d2 = faults[(partner,s)]
      #if len(d1) != len(d2) or d1[0].split()[1:] != d2[0].split()[1:]:
      #   print 'WARNING - different sg data or disk position from', h, 'and', partner, 'for', s
      if len(d1) != len(d2) or d1[0].split()[1] != d2[0].split()[1] or d1[0].split()[3:] != d2[0].split()[3:]: # ignore sg*
         print 'WARNING - different data or disk position from', h, 'and', partner, 'for', s
         print d1, d2
      else:
         for i in range(1,len(d1)):
            if d1[i].strip() != d2[i].strip():
               # sas disks report data read/written in the same lines as
               # errors, so attempt to ignore fields that look like ####.###
               diff = 0
               dd1 = d1[i].strip().split()
               dd2 = d2[i].strip().split()
               for j in range(len(dd1)):
                  if dd1[j] == dd2[j]:
                     continue
                  if len(dd1[j].split('.')) != 2 and len(dd2[j].split('.')) != 2:
                     continue
                  try:
                     a = float(dd1[j])
                     b = float(dd2[j])
                  except:
                     diff = 1
               if diff:
                  print 'WARNING - different data from', h, 'and', partner, 'for', s
                  print d1, d2

   if s not in printed:
      if not quiet:
         for d in d1:
            print d
      else:
         bad = 0
         for d in d1:
            #print d
            if rSerious.search(d):
               #print 'serious', d
               bad += 1
         if bad:
            for d in d1:
               print d
      printed.append(s)
