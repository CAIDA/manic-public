from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys
from lnk_db_defs import *
from meta import Base
from collections import defaultdict
import radix
import re
import getopt
import os
from myexceptions import *

#defaults to targ2dest, i.e., split targets
link2dest=0

try:
    opts, args = getopt.getopt(sys.argv[1:], "ho:l", ["help", "output=", "link2dest"])
except getopt.GetoptError as err:
    print str(err) 
    usage()
    sys.exit(2)

for o, a in opts:
    if o in ("-h", "--help"):
        usage()
        sys.exit(2)
    elif o in ("-l","--link2dest"):
        link2dest=1
    elif o in ("-o", "--output"):
        outfn = a
    else:
        assert False, "unhandled option"

db_fn = args[0]
checksum = args[1]
pfx2AS_fn = args[2]

try: 
    if os.stat(db_fn).st_size == 0:
        raise FileEmptyError('DB file is empty')
    engine = create_engine('sqlite:///'+db_fn)
    Session = sessionmaker(bind=engine)
    s = Session()
except OSError as o:
    #if DB file doesn't exist, no targets to extract
    sys.stderr.write('DB error: %s\n' % o)
    sys.exit(1)
except FileEmptyError as f:
    #if DB file empty, no targets to extract
    sys.stderr.write('DB error: %s\n' %f)
    sys.exit(1)

outfh = open(outfn, 'w') if outfn else sys.stdout

no_pfx2asfile=0
rtree = radix.Radix()
try: 
    if os.stat(pfx2AS_fn).st_size == 0:
        raise FileEmptyError('file is empty')
    sys.stderr.write('reading pfx2AS file %s\n' % pfx2AS_fn)
    PFX2AS = open(pfx2AS_fn,'r')
except OSError as o:
    sys.stderr.write('pfx2AS file error: %s\n' % o)
    sys.stderr.write('proceeding without pfx2AS file\n')
    no_pfx2asfile=1
except FileEmptyError as f:
    #since without this file there is nothing more to update in the DB, exit
    sys.stderr.write('pfx2AS file error: %s\n' %f)
    sys.stderr.write('proceeding without pfx2AS file\n')
    no_pfx2asfile=1
except IOError as i:
    #since without this file there is nothing more to update in the DB, exit
    sys.stderr.write('File open failed: %s\n' % i)
    sys.stderr.write('proceeding without pfx2AS file\n')
    no_pfx2asfile=1
else:    
    for line in PFX2AS:
        if re.match(r'#', line): continue
        fields = line.strip().split()
        if len(fields) != 3: continue
        rnode = rtree.add(fields[0]+'/'+fields[1])
        rnode.data["origin"] = fields[2]
    PFX2AS.close()

int_sum = int(checksum,0)

lnk_db_obj = {}
for l in s.query(Links).all():
    lnk_db_obj[l.id] = l

for dtl in s.query(Dest2Link).all():
    #sys.stderr.write('destination %s\n' % dtl.dest)
    #get the first and second IP of the link
    first = lnk_db_obj[dtl.linkid].first
    second = lnk_db_obj[dtl.linkid].second
    #sys.stderr.write('link %d first %s second %s\n' %(dtl.linkid,first,second))

    #get the origin AS of the probed destination
    if no_pfx2asfile:
        dest_AS = "UNK"
    else:
        rnode = rtree.search_best(dtl.dest)
        if rnode is None:
            dest_AS = "UNK"
        else:
            dest_AS = rnode.data["origin"]
        
    if link2dest:
        outfh.write('%s %s %s %d %d %d %d %s %s\n' 
                    %(first,second,dtl.dest,dtl.dist,int_sum,dtl.lastseen,
                      dtl.lastupdated,lnk_db_obj[dtl.linkid].asn,dest_AS))
    else:
        outfh.write('%s %s %d %d %d %d %s %s\n' 
                    %(first,dtl.dest,dtl.dist,int_sum,dtl.lastseen,
                      dtl.lastupdated,lnk_db_obj[dtl.linkid].asn,dest_AS))
        outfh.write('%s %s %d %d %d %d %s %s\n' 
                    %(second,dtl.dest,dtl.dist+1,int_sum,dtl.lastseen,
                      dtl.lastupdated,lnk_db_obj[dtl.linkid].asn,dest_AS))

if outfh is not sys.stdout:
    outfh.close()

sys.exit(0)
