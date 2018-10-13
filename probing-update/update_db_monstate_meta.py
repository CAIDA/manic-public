#Updates the monitor's state DB with various meta information:
#1) mapping from probed link to the destinations that were used to
#   probe that link at this update timestamp
#2) set of links that we started to probe at this update timestamp

from sqlalchemy import create_engine
from sqlalchemy import distinct
from sqlalchemy.orm import sessionmaker
import sys
from lnk_db_defs import *
from meta import Base
from collections import defaultdict
import getopt
import os
from myexceptions import *

try:
    opts, args = getopt.getopt(sys.argv[1:], "hl",
                               ["help", "link2dest"])
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
    else:
        assert False, "unhandled option"

db_fn = args[0]
targ2dest_fn = args[1]
timestamp = int(args[2])

try: 
    if os.stat(db_fn).st_size == 0:
        raise FileEmptyError('DB file is empty')
    engine = create_engine('sqlite:///'+db_fn)
    Session = sessionmaker(bind=engine)
    s = Session()
except OSError as o:
    sys.stderr.write('DB error: %s\n' % o)
    sys.exit(1)
except FileEmptyError as f:
    sys.stderr.write('DB error: %s\n' %f)
    sys.exit(1)
    
#get the set of links from the DB
lnk_db_obj={}
for l in s.query(Links).all():
    t_lnk = (l.first, l.second, l.asn)
    lnk_db_obj[t_lnk] = l

#get the set of probing objects from the DB
db_probing={}
for pr in s.query(Probing).all():
    db_probing[pr.ts_start,pr.linkid]=1

#get the set of link2dest or targ2dest objects from the DB
if link2dest:
    db_targlink2dest={}
    for l2d in s.query(TargLink2Dest).all():
        db_targlink2dest[l2d.ts,l2d.linkid]=1
else:
    db_targint2dest={}
    for i2d in s.query(TargInt2Dest).all():
        db_targint2dest[i2d.ts,i2d.target]=1

#get the set of links that are currently on the path to any
#destination, this is the set of links that we will start to probe at
#the current update timestamp
#insert the linkid and current timestamp into the probing table
for dtl in s.query(Dest2Link.linkid).distinct():
    sys.stderr.write('link in current probe set %d\n' % dtl.linkid)
    if (timestamp,dtl.linkid) not in db_probing:
        new_probing = Probing(ts_start=timestamp,linkid=dtl.linkid)
        sys.stderr.write('new probing with ts_start=%d, linkid=%d\n'
                         %(timestamp,dtl.linkid))
        s.add(new_probing)
        s.flush()
s.commit()

try: 
    if os.stat(targ2dest_fn).st_size == 0:
        raise FileEmptyError('file is empty')
    sys.stderr.write('reading targ2dest file %s\n' % targ2dest_fn)
    CURR = open(targ2dest_fn,'r')
except OSError as o:
    #since without this file there is nothing more to update in the DB, exit
    sys.stderr.write('Targ2dest file error: %s\n' % o)
    sys.stderr.write('stopping here\n')
    sys.exit(1)
except FileEmptyError as f:
    #since without this file there is nothing more to update in the DB, exit
    sys.stderr.write('Targ2dest file error: %s\n' %f)
    sys.stderr.write('stopping here\n')
    sys.exit(0)
except IOError as i:
    #since without this file there is nothing more to update in the DB, exit
    sys.stderr.write('File open failed: %s\n' % i)
    sys.stderr.write('stopping here\n')
    sys.exit(1)
else:    
    for line in CURR:
        fields = line.strip().split()
        if len(fields) != 6:
            sys.stderr.write('Invalid format: %s\n' %line.strip())
            continue
        if link2dest:
            (ip1,ip2,t_dest,t_dist,l_asn,csum) = fields
            targ_lnk = (ip1,ip2,l_asn)
            targ_lnk_id = lnk_db_obj[targ_lnk].id
            if (timestamp,targ_lnk_id) not in db_targlink2dest:
                new_targlink2dest = TargLink2Dest(ts=timestamp,
                                                  linkid=targ_lnk_id,
                                                  dest=t_dest)
                sys.stderr.write('new targlink2dest with ts=%d' % timestamp
                                 + ' linkid=%d dest=%s\n'
                                 %(targ_lnk_id,t_dest))
                s.add(new_targlink2dest)
                s.flush()
            else:
                sys.stderr.write('targlink2dest already in DB %d' % timestamp
                                 + ' linkid=%d dest=%s\n'
                                 %(targ_lnk_id,t_dest))
        else:
            (t_target,nan1,t_dest,t_dist,nan2,csum) = fields
            targ_int = (t_target,)                
            if (timestamp,t_target) not in db_targint2dest:
                new_targint2dest = TargInt2Dest(ts=timestamp,
                                                target=t_target,
                                                dest=t_dest)
                sys.stderr.write('new targint2dest with ts=%d' %timestamp
                                 + ' target=%s dest=%s\n'
                                 %(t_target,t_dest))
                s.add(new_targint2dest)
                s.flush()
            else:
                sys.stderr.write('targint2dest already in DB %d' % timestamp
                                 + ' target=%s dest=%s\n'
                                 %(t_target,t_dest))
    CURR.close()

s.commit()
sys.exit(0)    
