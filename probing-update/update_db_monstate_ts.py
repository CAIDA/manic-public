#Updates the monitor's state DB with a new TS mapping

from sqlalchemy import create_engine
from sqlalchemy import distinct
from sqlalchemy import exc
from sqlalchemy.orm import sessionmaker
import sys
from lnk_db_defs import *
from meta import Base
import getopt
import os
from myexceptions import *

if len(sys.argv) < 4:
    sys.stderr.write('Insufficient arguments to update_db_monstate_ts\n')
    sys.stderr.write('update_db_monstate_ts.py <db_fn> <warts_ts> <wall_ts>\n')
    sys.exit(1)

db_fn = sys.argv[1]
warts_ts = int(sys.argv[2])
wall_ts = int(sys.argv[3])

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
    
new_tsmap = TSMap(wartsts=warts_ts,wallts=wall_ts)
try:
    s.add(new_tsmap)
    s.flush()
    s.commit()
except exc.SQLAlchemyError as e:
    sys.stderr.write('DB error: %s\n' %e)
    sys.exit(1)

sys.exit(0)    
