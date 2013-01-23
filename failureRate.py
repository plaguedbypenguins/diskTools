#!/usr/bin/env python

# (c) Robin Humble. licensed under the GPL v3 or later

# look at disk serial numbers and track how they come and go.
# this gives us a pretty robust way of looking at disk failure rates

# the basic idea is to
#  - find new disks
#  - find disks that have gone away

# parsing many .gz files is slow, so a cached dict of disk serial
# numbers in each file is maintained

# additionally want to track
#  - machine/sas port(jbod)/disk slot where the disk failures occured
#    to see if rack location is correlated in any way
#     - we can assume that disks don't move around (much), so can store
#       this as a global dict of serial # vs. loc
#  - remapped sector counts
#     - store this with the disk serial number for each smart sample date

import os, gzip, sys, cPickle, time


# search these subdirectories for smartGather output and pickle files
dirList = ( '.', '2011', '2010', '2009' )

# disks that were pulled out of hamster25/26, replaced by hitachis, then used as spares
# so these aren't real failed disks unless they disappear for a 2nd time...
jbodReplaced = {
    # hamster25/26 port 3
  '2012-05-17-18:00:48' : [ "9QJ2ZV01", "9QJ3RG9V", "9QJ3S5R2", "9QJ3X2ZV", "9QJ3XDMH",
    "9QJ3XE01", "9QJ3XWC6", "9QJ3Y4KT", "9QJ3Y53Z", "9QJ3YFPP", "9QJ3YFRB", "9QJ3YFYZ", "9QJ3YJCB", "9QJ3YKKE",
    "9QJ3YKKN", "9QJ3ZKF4", "9QJ41C53", "9QJ41CC3", "9QJ41JP6", "9QJ41K38", "9QJ42NMT", "9QJ60MQZ", "9QJ6QQ4G",  # seagates removed
    # 9QJ3XZQR was the 24th removed. it was already dead though, so it's a real failure
    "JPW9K0J81VYRBL", "JPW9K0J81YWK0L", "JPW9K0J823VHKL", "JPW9K0J826771L", "JPW9K0J8269MLL", "JPW9K0J827L4AL",
    "JPW9K0J828HV1L", "JPW9K0J82A5N8L", "JPW9K0J82A6B2L", "JPW9K0J82A6EJL", "JPW9K0J82AK20L", "JPW9K0J82AKEDL",
    "JPW9K0J82AWP3L", "JPW9K0J82B0J6L", "JPW9K0J82B32AL", "JPW9K0J82B40XL", "JPW9K0J82B4G5L", "JPW9K0J82B4X5L",
    "JPW9K0J82B52UL", "JPW9K0J82B6K1L", "JPW9K0J82B6WVL", "JPW9K0J82B6YRL", "JPW9K0J82GWS8L" ],   # hitachis added
  '2012-05-15-16:35:48' : [ "JPW9K0J82B373L" ], # weird transition disk labelling due to 9QJ3XZQR deletion, actually was in port 3 on 2012-05-17-18:00:48 like the rest

   # hamster25/26 port 2
  '2012-06-14-17:38:24' : [ "JPW9K0J81VYRBL", "JPW9K0J81YWK0L", "JPW9K0J823VHKL", "JPW9K0J826771L", "JPW9K0J8269MLL", "JPW9K0J827L4AL",
    "JPW9K0J828HV1L", "JPW9K0J82A5N8L", "JPW9K0J82A6B2L", "JPW9K0J82A6EJL", "JPW9K0J82AK20L", "JPW9K0J82AKEDL",
    "JPW9K0J82AWP3L", "JPW9K0J82B0J6L", "JPW9K0J82B32AL", "JPW9K0J82B40XL", "JPW9K0J82B4G5L", "JPW9K0J82B4X5L",
    "JPW9K0J82B52UL", "JPW9K0J82B6K1L", "JPW9K0J82B6WVL", "JPW9K0J82B6YRL", "JPW9K0J82GWS8L", "JPW9K0J82B373L" ],
      # (a) weird transition disk labelling made it look like the above went away for the week that were adding the below
  '2012-06-22-17:33:21' : [ "9QJ3YFV1", "9QJ41ADG", "9QJ4N58N", "9QJ4P9KE", "9QJ4PHN4", "9QJ4PT5H",
    "9QJ4QH5X", "9QJ4R74E", "9QJ4T53Z", "9QJ4T558", "9QJ4T5GL", "9QJ4T5QR", "9QJ4T65X", "9QJ4TMKT", "9QJ4V4KQ",
    "9QJ4VYBC", "9QJ4WQ47", "9QJ4YWCR", "9QJ4YWFH", "9QJ4YWML", "9QJ4YYQW", "9QJ4ZHKF", "9QJ5NHXD", "9QJ5Z2A9",  # seagates removed
    "JPW9K0J82H1E6L", "JPW9K0J82GVL0L", "JPW9K0J82GVB8L", "JPW9K0J82GXDJL", "JPW9K0J82GVGML", "JPW9K0J82H15JL",
    "JPW9K0J826X96L", "JPW9K0J81R619L", "JPW9K0J82GVH4L", "JPW9K0J82B42KL", "JPW9K0J81R605L", "JPW9K0J824V28L",
    "JPW9K0J81YW48L", "JPW9K0J82GVK4L", "JPW9K0J82GVGTL", "JPW9K0J824496L", "JPW9K0J82GVWEL", "JPW9K0J820KPML",
    "JPW9K0J81YW73L", "JPW9K0J82GW4PL", "JPW9K0J81TP0BL", "JPW9K0J82GU6EL", "JPW9K0J825ULHL", "JPW9K0J81VG8ZL",  # hitachis added
    "JPW9K0J81VYRBL", "JPW9K0J81YWK0L", "JPW9K0J823VHKL", "JPW9K0J826771L", "JPW9K0J8269MLL", "JPW9K0J827L4AL",
    "JPW9K0J828HV1L", "JPW9K0J82A5N8L", "JPW9K0J82A6B2L", "JPW9K0J82A6EJL", "JPW9K0J82AK20L", "JPW9K0J82AKEDL",
    "JPW9K0J82AWP3L", "JPW9K0J82B0J6L", "JPW9K0J82B32AL", "JPW9K0J82B40XL", "JPW9K0J82B4G5L", "JPW9K0J82B4X5L",
    "JPW9K0J82B52UL", "JPW9K0J82B6K1L", "JPW9K0J82B6WVL", "JPW9K0J82B6YRL", "JPW9K0J82GWS8L", "JPW9K0J82B373L" ],  # as per (a)

   # hamster25/26 port 1
   '2012-09-05-15:33:58' : [ "JPW9K0J81TJHDL", "JPW9K0J81VZH2L", "JPW9K0J825N0RL", "JPW9K0J82A0UXL", "JPW9K0J82AZW9L", # hitachis added
     "JPW9K0J82B4SVL", "JPW9K0J82B6H1L", "JPW9K0J82GV4XL", "JPW9K0J82GVLYL", "JPW9K0J82GVX3L", "JPW9K0J82GWHML",
     "JPW9K0J82J8ULL", "JPW9K0J82J9WPL", "JPW9K0J82JDS8L", "JPW9K0J82JGSHL", "JPW9K0J82JPTLL", "JPW9K0J82JV1JL",
     "JPW9K0J82JW4GL", "JPW9K0J82JYBNL", "JPW9K0J82JYELL", "JPW9K0J82K0X0L", "JPW9K0J82K2T5L", "JPW9K0J82K3B3L",
     "JPW9K0J82K3TGL" ],
   '2012-09-17-17:37:43' : [ "9QJ3J0LZ", "9QJ3XD88", "9QJ41JGD", "9QJ4H00M", "9QJ4PWBF", "9QJ4Q2VS", "9QJ4QXTM",  # seagates removed
     "9QJ4R61V", "9QJ4S6BK", "9QJ4S939", "9QJ4T5B2", "9QJ4T5RB", "9QJ4TM11", "9QJ4TMKT", "9QJ4TZDW", "9QJ4TZH7",
     "9QJ4VWGX", "9QJ4VY0V", "9QJ4YXC3", "9QJ4YXKZ", "9QJ4YXPM", "9QJ4YXQP", "9QJ60HGY", "9QJ63WTH" ],

   # hamster25/26 port 0
   '2012-10-23-13:59:08' : [ "JPW9K0J826KGBL", "JPW9K0J8284BEL", "JPW9K0J82A6BLL", "JPW9K0J82AW4JL", "JPW9K0J82AW9BL",  # hitachis added
     "JPW9K0J82B00SL", "JPW9K0J82B02HL", "JPW9K0J82B09SL", "JPW9K0J82B38HL", "JPW9K0J82B3JTL", "JPW9K0J82B3KBL",
     "JPW9K0J82B6JAL", "JPW9K0J82BRN6L", "JPW9K0J82E9JUL", "JPW9K0J82GVJEL", "JPW9K0J82GVS0L", "JPW9K0J82H17XL",
     "JPW9K0J82H1H8L", "JPW9K0J82JPBLL", "JPW9K0J82JPE5L", "JPW9K0J82JULAL", "JPW9K0J82K2ALL", "JPW9K0J82K3V8L",
     "JPW9K0N11A1X4L" ],
   '2012-11-02-17:35:58' : [ "9QJ3YJK8", "9QJ4M5QG", "9QJ4N58N", "9QJ4RLNJ", "9QJ4T58B", "9QJ4T5H0",  # seagates removes
                             "9QJ4T5RE", "9QJ4T5XA", "9QJ4TLWV", "9QJ4TMCV", "9QJ4TMYN", "9QJ4V4FH",
                             "9QJ4YT2V", "9QJ4YX0P", "9QJ4YXC1", "9QJ4YXDE", "9QJ4YZKR", "9QJ4Z0P4",
                             "9QJ5L3FS", "9QJ5QGVJ", "9QJ5RVGC", "9QJ60HNQ", "9QJ60L8Y", "9QJ8AGT0" ],
   # hamster 23/24 port 3
   '2012-11-23-16:59:20' : [ "JPW9K0J81PV4GL", "JPW9K0J81VVTPL", "JPW9K0J828347L", "JPW9K0J82A6D3L", "JPW9K0J82B09YL", "JPW9K0J82B0DUL",  # hitachis added
                             "JPW9K0J82B46WL", "JPW9K0J82B4HBL", "JPW9K0J82B53UL", "JPW9K0J82H15BL", "JPW9K0J82JPBSL", "JPW9K0J82JPG7L",
                             "JPW9K0J82JRU6L", "JPW9K0J82JUWEL", "JPW9K0J82JVKGL", "JPW9K0J82K1P2L", "JPW9K0N110B4TL", "JPW9K0N110X7DL",
                             "JPW9K0N119S2JL", "JPW9K0N119SEAL", "JPW9K0N119TUAL", "JPW9K0N119VARL", "JPW9K0N119ZXSL", "JPW9K0N11A1V1L" ],
   '2012-12-03-17:29:43' : [ "9QJ4T9PV", "9QJ4TAP4", "9QJ4TLYX", "9QJ4TZEH", "9QJ4V03D", "9QJ4VY6A",   # seagates removed
                             "9QJ4VYBV", "9QJ4YSRB", "9QJ4YSRL", "9QJ4YT0H", "9QJ4YT5R", "9QJ4YT7Z",
                             "9QJ4YY0J", "9QJ4YZ14", "9QJ4YZ18", "9QJ4YZ22", "9QJ4YZ8A", "9QJ4YZGS",
                             "9QJ4YZGW", "9QJ4YZPC", "9QJ4YZT5", "9QJ4Z042", "9QJ4ZHQT", "9QJ4ZHQZ" ],
   # hamster23/24 port 2
   '2013-01-05-12:51:39' : [ "9QJ3YFWY", "9QJ3YKKE", "9QJ3YN16", "9QJ41JT9", "9QJ4LSAY", "9QJ4S6N2",  # seagates removed (actually when hitachis were added, but this gave errors and made the seagates disappear)
                             "9QJ4T4Q0", "9QJ4T558", "9QJ4T55L", "9QJ4T59Y", "9QJ4T5XA", "9QJ4T65J",
                             "9QJ4TZMN", "9QJ4TZWZ", "9QJ4VV0H", "9QJ4VVLR", "9QJ4VW98", "9QJ4VWGW",
                             "9QJ4VWH3", "9QJ4VXLY", "9QJ4W0N0", "9QJ4WA79", "9QJ5WTZ3", "9QJ6SHFM" ],
   '2013-01-15-18:05:16' : [ "JPW9K0HZ0VWXXL", "JPW9K0J82AK1HL", "JPW9K0J82AWN0L", "JPW9K0J82B0UPL", "JPW9K0J82B31ML", "JPW9K0J82B3ZJL",   # hitachis added
                             "JPW9K0J82B4HEL", "JPW9K0J82B6R4L", "JPW9K0J82BR18L", "JPW9K0J82BRPKL", "JPW9K0J82GV1LL", "JPW9K0J82JG4LL",
                             "JPW9K0J82JRSPL", "JPW9K0J82JUR5L", "JPW9K0J82JZBVL", "JPW9K0J82K1GPL", "JPW9K0N119WJ8L", "JPW9K0N11AWZBL",
                             "JPW9K0N11AX06L", "JPW9K0N11AX2BL", "JPW9K0N11B159L", "JPW9K0N11B16UL", "JPW9K0N11B1D2L", "JPW9K0N11B1HBL" ]
}

def uniq( list ):
   l = []
   prev = None
   for i in list:
      if i != prev:
         l.append( i )
      prev = i
   return l

allDb = {}
allLoc = {}     # disk location by serial

for d in dirList:
   print 'dir', d
   if not os.path.exists( d ):
      continue

   t = time.time()
   new = 0

   # slurp up the location so far - assume is in sync with db - BAD assumption, but hey
   locPf = d + '/' + 'loc.pickle'
   try:
      loc = cPickle.load(open(locPf, 'rb'))
   except:
      loc = {}

   # slurp up the serial number db so far
   pf = d + '/' + 'fail.pickle'
   try:
      db = cPickle.load(open(pf, 'rb'))
   except:
      db = {}

   # check that all disks have a loc
   print 'checking all cached disks have a loc'
   err = 0
   ser = []
   kk = db.keys()
   kk.sort()
   for k in kk:
      rem = db[k]
      prevSer = ser
      ser = rem.keys()
      if ser == prevSer: # optimise away the case where disks don't change
         continue
      for s in ser:
         if s not in loc.keys():
            err = 1
            print 'WARNING: not all disks have a location. redoing loc and db build due to', s, k
      if err:
         db = {}
         loc = {}
         break

   # check that all locs are in the db too
   print 'checking all cached locations have a disk'
   for s in loc.keys():
      err = 1
      ser = []
      kk = db.keys()
      kk.sort()
      for k in kk:
         rem = db[k]
         prevSer = ser
         ser = rem.keys()
         if ser == prevSer: # optimise away the case where disks don't change
            err = 0
            continue
         if s in ser:
            err = 0   # found the disk in at least one smart log
            break
      if err:
         db = {}
         loc = {}
         print 'WARNING: not all disks with locations are in the disk db. redoing loc and db build'
         break

   # slurp up all smart.*.gz files (eg. smart.2011-05-13-12:53:15.gz)
   for f in os.listdir( d ):
      s = f.split('.')
      if len(s) == 3 and s[0] == 'smart' and s[2] == 'gz':
         key = s[1]
         if key in db.keys():
            continue

         print f
         new += 1
         g = gzip.open( d + '/' + f, 'rb')
         r = g.read()
         g.close()

         serial = ''
         #se = []   # serial numbers
         rem = {}  # remapped sectors by serial number
         for l in r.split('\n'):
            if len(l) > 0 and l[0] == '-' and 'gopher4' in l:
               #print 'found gopher4 disks. skipping rest of file'
               break

            # order of the lines in the smart data files is
            #    header, serial, realloc
            # hence the logic of the below

            if l[:7] == 'hamster' or l[:6] == 'marmot':
               # hamster1 /dev/sdh /dev/sg9 port 0 disk 5
               #   or older ones use
               # hamster1 /dev/sdc port 0 disk 0
               li = l.split()
               mach = li[0]
               try:
                  port = int(li[4])
                  diskNum = int(li[6])
               except:
                  try:
                     port = int(li[3])
                     diskNum = int(li[5])
                  except:
                     print 'failed to parse', l
                     sys.exit(1)
            elif l[:25] == '  5 Reallocated_Sector_Ct':
               li = l.split()
               #  5 Reallocated_Sector_Ct   0x0033   100   100   036    Pre-fail  Always       -       0
               try:
                  sectors = int(li[-1])
               except:
                  print 'read of sectors failed. line', l, 'disk', serial
                  #sys.exit(1)
                  continue
               rem[serial] = sectors
               serial = ''
               sectors = ''
            elif 'Serial' in l:
               li = l.split()
               # only look at SATA disks. SAS have 4 fields.
               if len(li) == 3 and li[0] == 'Serial' and li[1] == 'Number:':
                  serial = li[2]
                  #se.append(serial)
                  # update this anyway, even though it's ~always the same - want the last if it does change
                  loc[serial] = ( mach, port, diskNum )

         #se.sort()
         #se = uniq(se)
         #print len(se)
         #db[key] = ( se, rem )   # put serial numbers and remapped count into the db
                           # this is redundant as remapped is indexed by serial, but hey
         db[key] = rem  # put serial numbers and remapped count into the db

         if time.time() > t + 30:
            print 'dumping intermediate serial/sectors db'
            cPickle.dump(db, open(pf + '.tmp','wb'))
            os.rename(pf + '.tmp', pf)

            print 'dumping intermediate loc db'
            cPickle.dump(loc, open(locPf + '.tmp','wb'))
            os.rename(locPf + '.tmp', locPf)

            t = time.time()

   if new:
      print 'dumping final serial/sectors db'
      cPickle.dump(db, open(pf + '.tmp','wb'))
      os.rename(pf + '.tmp', pf)

      print 'dumping loc db'
      cPickle.dump(loc, open(locPf + '.tmp','wb'))
      os.rename(locPf + '.tmp', locPf)

   print 'read', len(db.keys()), 'files. cached', len(db.keys())-new, 'new', new
   allDb.update(db)
   allLoc.update(loc)

db = allDb
loc = allLoc
#print 'db', db
#print 'loc', loc

# order files in time
keys = db.keys()
keys.sort()

print 'found', len(keys), 'disk sweep files to look through'

# find how serial numbers change from one to the next
first = 1
prev = []
prevK = ''
allSoFar = {}
newdisks = []
lastSectors = {}
for k in keys:
   if first:
      prevRem = db[k]
      prev = prevRem.keys() # serial numbers
      prev.sort()
      prevK = k
      for p in prev:
         allSoFar[p] = k
      first = 0
   rem = db[k]
   a = rem.keys()
   a.sort()

   # optimise away the common case of all disks the same
   if a == prev:
      prevK = k
      if rem == prevRem:
         continue
      # except need to keep track of the last remapped sector counts...
      for s in a:
         try:
            r = int(rem[s])
            lastSectors[s] = r
         except:
            pass
      continue

   # find disks that have gone away
   for s in prev:
      if s not in a:
         print k, s, '-',

         # handle the case of wholesale jbod replacement by hitachis
         if k in jbodReplaced.keys(): # jbod replace date
            if s in jbodReplaced[k]:
               print 'jbod replacement disk. not a real fail'
               continue

         try:
            print 'left with', prevRem[s], 'remapped sectors', 'location', loc[s], allSoFar[s]
         except:
            try:
               print lastSectors[s], 'remapped sectors (from prev)', 'location', loc[s], allSoFar[s]
            except:
               print 'unknown remapped sectors'
         #print k, s, '(gone away, previous', prevK, 'prev # disks', len(db[prevK]), ')'

   # find new disks
   remapped = 0
   for s in a:
      # keep track of the last remapped sector counts...
      try:
         r = int(rem[s])
         lastSectors[s] = r
         remapped += r
      except:
         print 'incrementing remapped failed. disk', s, 'rem[s]', rem[s]
         sys.exit(1)

      if s not in prev:
         print k, s, '+',

         # handle the case of wholesale jbod replacement by hitachis
         if k in jbodReplaced.keys(): # jbod replace date
            if s in jbodReplaced[k]:
               print 'jbod replacement disk. not a real fail'
               continue

         newdisks.append((k,s))
         print 'location', loc[s],
         if s in allSoFar.keys():
            print '(reappeared from prev', allSoFar[s], ')'
         else:
            allSoFar[s] = k
            print

   print k, 'remapped', remapped

   prev = a
   prevRem = rem
   prevK = k

# work out how many per month
monthly = {}
for f in newdisks:
   k, d = f
   ym = k[:7]  # eg. 2010-01
   if ym in monthly.keys():
      monthly[ym] += 1
   else:
      monthly[ym] = 1

# dump something that gnuplot can plot
keys = monthly.keys()
keys.sort()
cnt = 0
f = open('fails.month', 'w')
f.write('# disk replacements by month\n')
#print '# disk replacements by month'
for k in keys:
   #print k, monthly[k]
   f.write('%s %d\n' % (k,monthly[k]))
   cnt += monthly[k]
f.close()
#print open('fails.month').readlines()

# work out how many per week
week = {}
for f in newdisks:
   k, d = f
   t = time.strptime( k[:10], "%Y-%m-%d" ) # eg. 2010-01-27
   w = int(time.strftime( "%W", t )) + 1   # week of year - +1 as gnuplot starts from 1
   w = time.strftime( "%Y", t ) + "-%d" % ( 7*w )   # bins of week of year, eg. 2010-21
   if w in week.keys():
      week[w] += 1
   else:
      week[w] = 1

# dump something that gnuplot can plot
keys = week.keys()
keys.sort()
cnt = 0
f = open('fails.week', 'w')
f.write('# disk replacements by week\n')
for k in keys:
   f.write('%s %d\n' % (k,week[k]))
   cnt += week[k]
f.close()
#print open('fails.week').readlines()

print '# total', cnt, 'disks replaced'
print '# gnuplot with'
print 'set timefmt "%Y-%m"'
print 'set xdata time'
print 'set format x "%m\\n%Y"'
print 'set yrange [0:]'
print 'set style data linespoints'
print 'load "special"'
print 'plot "fails.month" using 1:2 with impulses title "%d disks replaced"' % cnt
print 
print 'set timefmt "%Y-%m"'
print 'load "special"'
print 'set timefmt "%Y-%j"'
print '#set xrange ["2011-215":]'
print 'plot "fails.week" using 1:2 with impulses title "weekly"'
