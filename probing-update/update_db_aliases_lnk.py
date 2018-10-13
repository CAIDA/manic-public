from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys
import os
from lnk_db_defs import *
from meta import Base
from collections import defaultdict
from myexceptions import *

db_fn = sys.argv[1]
aliases_fn = sys.argv[2]
timestamp = int(sys.argv[3])

try: 
    if os.stat(db_fn).st_size == 0:
        raise FileEmptyError('DB file is empty')
    engine = create_engine('sqlite:///'+db_fn)
    Session = sessionmaker(bind=engine)
    s = Session()
except OSError as o:
    sys.stderr.write('DB error: %s\n' % o)
    exit(1)
except FileEmptyError as f:
    sys.stderr.write('DB error: %s\n' %f)
    exit(1)

try: 
    if os.stat(aliases_fn).st_size == 0:
        raise FileEmptyError('file is empty')
    sys.stderr.write('reading aliases file %s\n' % aliases_fn)
    ALIASES = open(aliases_fn,'r')
except OSError as o:
    sys.stderr.write('Aliases file error: %s\n' % o)
    #since without this file there is nothing to update in the DB, exit
    exit(1)
except FileEmptyError as f:
    sys.stderr.write('Aliases file error: %s\n' %f)
    #since without this file there is nothing to update in the DB, exit
    exit(1)
except IOError as i:
    sys.stderr.write('File open failed: %s\n' % i)
    #since without this file there is nothing to update in the DB, exit
    exit(1)
else:
    router_id = 0
    for line in ALIASES:
        fields = line.strip().split()
        for interface in fields:
            new_alias_row = Aliases(ts=timestamp,ip=interface,routerid=router_id)
            s.add(new_alias_row)
        s.flush()
        router_id+=1
    ALIASES.close()

s.commit()        
exit(0)
