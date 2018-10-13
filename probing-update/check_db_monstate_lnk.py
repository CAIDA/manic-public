from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys
import os
import getopt
from lnk_db_defs import *
from meta import Base
from collections import defaultdict

def connect_db(db_fn):
    try: 
        engine = create_engine('sqlite:///'+db_fn)
        Session = sessionmaker(bind=engine)
        ses = Session()
    except OSError as o:
        sys.stderr.write('DB error: %s\n' % o)
        exit(1)
    return ses

def main():
    db_fn=None
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], 
                                   "hdb:l:r:t:", 
                                   ["help", "debug", 
                                    "db="])
    except getopt.GetoptError as err:
        print str(err)
        sys.exit(1)

    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-d","--debug"):
            DEBUG=True
        elif o in ("-b","--db"):
            db_fn = a
        else:
            assert False, "unhandled option"

    ts = int(args[0])
            
    if not db_fn:
        sys.stderr.write('db file must be provided\n')
        sys.exit(1)
    
    #connect to the sqlite database
    s = connect_db(db_fn)
    
    d2l_dests = {}
    d2l_links = {}
    dup_dest =0 
    max_lseen_diff = 0 
    for dtl in s.query(Dest2Link).all():
        if dtl.dest in d2l_dests:
            dup_dest += 1
        d2l_links[dtl.linkid] = 1
        lseen = dtl.lastseen
        if ts - lseen > max_lseen_diff: 
            max_lseen_diff = ts - lseen

    print "dup_dest", dup_dest
    print "links probed",len(d2l_links.keys())
    print "max_lseen_diff", max_lseen_diff
    
    exit(0)

if __name__ == "__main__":
    main()
