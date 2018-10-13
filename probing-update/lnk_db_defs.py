from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from meta import Base

class Aliases(Base):
    __tablename__='aliases'
    ts = Column(Integer, primary_key=True)
    ip = Column(String, primary_key=True, index=True)
    routerid = Column(Integer)

class Links(Base):
    __tablename__='links'
    id = Column(Integer, primary_key=True, autoincrement=True)
    first = Column(String)
    second = Column(String)
    asn = Column(String)

class LinkInference(Base):
    __tablename__='linkinference'
    id = Column(Integer,primary_key=True,autoincrement=True)
    ts = Column(Integer, primary_key=True, index=True)
    reason = Column(String)

class Triplets(Base):
    __tablename__='triplets'
    id = Column(Integer, primary_key=True, autoincrement=True)
    first = Column(String)
    second = Column(String)
    third = Column(String)

class Dest2Link(Base):
    __tablename__='dest2link'
    #id = Column(Integer, primary_key=True, autoincrement=True)
    dest = Column(String, index=True, primary_key=True)
    linkid = Column(Integer, ForeignKey("links.id"), primary_key=True)
    dist = Column(Integer)
    lastseen = Column(Integer)
    lastupdated = Column(Integer)

class Probing(Base):
    __tablename__='probing'
    #id = Column(Integer, primary_key=True, autoincrement=True)
    linkid = Column(Integer, ForeignKey("links.id"), primary_key=True)
    ts_start = Column(Integer, primary_key=True)
    ts_end = Column(Integer)

class TargInt2Dest(Base):
    __tablename__='targint2dest'
    ts = Column(Integer, primary_key=True, index=True)
    target = Column(String, primary_key=True, index=True)
    dest = Column(String, ForeignKey("dest2link.dest"), 
                  primary_key=True, index=True)

class TargLink2Dest(Base):
    __tablename__='targlink2dest'
    ts = Column(Integer, primary_key=True, index=True)
    linkid = Column(Integer, primary_key=True, index=True)
    dest = Column(String, ForeignKey("dest2link.dest"), 
                  primary_key=True, index=True)

class DNS(Base):
    __tablename__='dns'
    ts = Column(Integer, primary_key=True)
    ip = Column(String, primary_key=True, index=True)
    name = Column(String)

class TSMap(Base):
    __tablename__='tsmap'
    wartsts = Column(Integer, primary_key=True, index=True)
    wallts = Column(Integer, primary_key=True, index=True)
