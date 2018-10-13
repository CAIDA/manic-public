from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys
import lnk_db_defs
from meta import Base

db_name = sys.argv[1]

engine = create_engine('sqlite:///'+db_name)

Base.metadata.create_all(engine)
