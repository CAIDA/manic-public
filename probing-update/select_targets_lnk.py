import sys
import os
import re
from collections import defaultdict
import getopt
from myexceptions import *

intmatch = re.compile('^\d+$');

def getASDiff(as1,as2):
    diff=None
    if ((as1 == "UNK") or (as2 == "UNK") or 
        (not intmatch.match(as1)) or (not intmatch.match(as2))):
        diff = 100000;
    else:
        diff=abs(int(as1)-int(as2));

    return diff

def getLastOctet(ip):
    octets = ip.split('.')
    return int(octets[-1])

try:
    opts, args = getopt.getopt(sys.argv[1:], "ho:lm",
                               ["help", "output=", "link2dest", "maxdest"])
except getopt.GetoptError as err:
    print str(err) 
    usage()
    sys.exit(2)

link2dest=0
outfn=None
maxdest=3
    
for o, a in opts:
    if o in ("-h", "--help"):
        usage()
        sys.exit(2)
    elif o in ("-l","--link2dest"):
        link2dest=1
    elif o in ("-o", "--output"):
        outfn = a
    elif o in ("-m", "--maxdest"):
        maxdest = int(a)
    else:
        assert False, "unhandled option"

outfh = open(outfn, 'w') if outfn else sys.stdout

raw_targ_fn = args[0]
curr_targ_fn = args[1]
checksum = args[2]

int_sum = int(checksum,0)

curr_distance=defaultdict(dict)
new_distance=defaultdict(dict)
final_distance=defaultdict(dict)
new_diffAS=defaultdict(dict)
new_destAS=defaultdict(dict)

#read the current targets file 
try:
    if os.stat(raw_targ_fn).st_size == 0:
        raise FileEmptyError('file is empty')
    sys.stderr.write('reading curr target file %s\n' % curr_targ_fn)
    CURR = open(curr_targ_fn,'r')    
except OSError as o:
    #can continue without this file
    sys.stderr.write('curr_target_file error: %s\n' % o)
    sys.stderr.write('assuming empty curr_target_file\n')
except FileEmptyError as f:
    #can continue without this file
    sys.stderr.write('curr_target_file error: %s\n' %f)
    sys.stderr.write('assuming empty curr_target_file\n')
except IOError as i:
    #can continue without this file
    sys.stderr.write('File open failed: %s\n' % i)
    sys.stderr.write('assuming empty curr_target_file\n')
else:
    for line in CURR:
        fields = line.strip().split()
        if len(fields) != 6:
            sys.stderr.write('Invalid line format: %s\n' %line.strip())
            continue
        (ip1,ip2,dest,dist,l_asn,csum) = fields
        if link2dest:
            targ = (ip1,ip2,l_asn)
        else:
            targ = (ip1,)
        curr_distance[targ][dest]=int(dist)
    CURR.close()

#read the raw targets file 
try: 
    if os.stat(raw_targ_fn).st_size == 0:
        raise FileEmptyError('file is empty')
    sys.stderr.write('reading raw target file %s\n' % raw_targ_fn)
    RAW = open(raw_targ_fn,'r')
except OSError as o:
    #since without this file there is nothing to do to select targets
    sys.stderr.write('raw_target_file error: %s\n' % o)
    sys.stderr.write('stopping here\n')
    sys.exit(1)
except FileEmptyError as f:
    #since without this file there is nothing to do to select targets
    sys.stderr.write('raw_target_file error: %s\n' %f)
    #sys.stderr.write('stopping here\n')
    #sys.exit(1)
except IOError as i:
    #since without this file there is nothing to do to select targets
    sys.stderr.write('File open failed: %s\n' % i)
    sys.stderr.write('stopping here\n')
    sys.exit(1)    
else: 
    for line in RAW:
        fields = line.strip().split()
        if len(fields) != 9 : continue
        (ip1,ip2,dest,dist,csum,ls,lu,l_asn,d_asn) = fields
        diffAS = getASDiff(l_asn,d_asn)

        if link2dest:
            targ = (ip1,ip2,l_asn)
            new_distance[targ][dest]=int(dist)
            new_destAS[targ][dest]=(l_asn,d_asn)
            new_diffAS[targ][dest] = diffAS
        else:
            targ1 = (ip1,)
            new_distance[targ1][dest]=int(dist)
            new_destAS[targ1][dest]=(l_asn,d_asn)
            new_diffAS[targ1][dest] = diffAS
            targ2 = (ip2,)
            new_distance[targ2][dest]=int(dist)+1
            new_destAS[targ2][dest]=(l_asn,d_asn)
            new_diffAS[targ2][dest] = diffAS
    RAW.close()

same_dest=0;
lost_dest=0;
added_dest=0

for targ in new_distance:
    n_dest_t=0
    if targ in curr_distance:
        sys.stderr.write('target %s currently probed\n' % str(targ));
        for dest in curr_distance[targ]:
            if dest in new_distance[targ]:
                sys.stderr.write('target %s destination %s curr_dist %d new_dist %d\n'
                                 % (str(targ), dest, curr_distance[targ][dest],
                                new_distance[targ][dest]))
                final_distance[targ][dest] = new_distance[targ][dest]
                n_dest_t+=1
                sys.stderr.write('target %s keeping destination %s with dist %d\n'
                             % (str(targ),dest,new_distance[targ][dest]))
                same_dest+=1
            else:
                sys.stderr.write('target %s destination %s curr_dist %d new_dist unavail\n'
                                 %(str(targ),dest,curr_distance[targ][dest]))
                lost_dest+=1
    
        for dest in sorted(new_distance[targ], key=lambda d: (new_diffAS[targ][d],getLastOctet(d))):
            sys.stderr.write('targ %s dest %s diff %d\n' 
                             % (str(targ),dest,new_diffAS[targ][dest]))
            if (n_dest_t < maxdest) and (dest not in final_distance[targ]):
                final_distance[targ][dest] = new_distance[targ][dest]
                n_dest_t+=1
                sys.stderr.write('target %s adding new destination %s dist %d\n'
                                 % (str(targ),dest,new_distance[targ][dest]))
                added_dest+=1

    else:
        sys.stderr.write('------------------------------\n')
        sys.stderr.write('target %s not currently probed\n' % str(targ))
        for dest in sorted(new_distance[targ], key=lambda d: (new_diffAS[targ][d],getLastOctet(d))):
            #sys.stderr.write('sorted dest %s\n' % dest)
            sys.stderr.write('targ %s dest %s diff %d\n' 
                             % (str(targ),dest,new_diffAS[targ][dest]))
            if (n_dest_t < maxdest) and (dest not in final_distance[targ]):
                final_distance[targ][dest] = new_distance[targ][dest]
                n_dest_t+=1
                sys.stderr.write('target %s adding new destination %s dist %d\n'
                                 % (str(targ),dest,new_distance[targ][dest]))
                added_dest+=1

for targ in curr_distance:
    if targ not in new_distance:
        sys.stderr.write('target %s not longer probed\n' % str(targ));
        for dest in curr_distance[targ]:
            lost_dest+=1
    
for targ in final_distance:
    for dest in final_distance[targ]:
        if link2dest:
            outfh.write('%s %s %s %d %s %d\n' % 
                        (targ[0],targ[1],dest,
                         final_distance[targ][dest],targ[2],int_sum))
        else:
            outfh.write('%s %s %s %d %d %d\n' % 
                        (targ[0],"foo",dest,
                         final_distance[targ][dest],0,int_sum))


if outfh is not sys.stdout:
    outfh.close()

sys.stderr.write('n_same %d n_lost %d n_added %d\n' %
                 (same_dest,lost_dest,added_dest))
sys.exit(0)
