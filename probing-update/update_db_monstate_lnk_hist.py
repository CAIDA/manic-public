# update the monstate DB with up to date status of links to be probed
# this version takes a link history file, which contains the number of 
# previous snapshots in which a link was observed and requires a link 
# to have been seen in a threshold number of previous snapshots to be 
# considered as "seen" in the current snapshot

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sys
import os
import getopt
from lnk_db_defs import *
from meta import Base
from collections import defaultdict
from myexceptions import *

#15 days
PURGETIME=1296000
HISTORY_THRESH=4
ONEOFF=False
DEBUG=False
id2link={}
link_db_obj = {}
link_history_cnt = {}
d2l_new = {}
d2l_new_props = {}
d2l_db = {}
d2l_db_props = {}
d2l_db_obj ={}

def connect_db(db_fn):
    try: 
        if os.stat(db_fn).st_size == 0:
            raise FileEmptyError('DB file is empty')
        sys.stderr.write('connecting to DB\n')
        engine = create_engine('sqlite:///'+db_fn)
        Session = sessionmaker(bind=engine)
        ses = Session()
    except OSError as o:
        sys.stderr.write('DB error: %s\n' % o)
        exit(1)
    except FileEmptyError as f:
        sys.stderr.write('DB error: %s\n' %f)
        exit(1)
    return ses

def get_links_from_db(ses):
    global link_db_obj
    global id2link
    sys.stderr.write('getting links from DB\n')
    for l in ses.query(Links).all():
        t_link = (l.first, l.second, l.asn)
        link_db_obj[t_link] = l
        id2link[l.id]= t_link

def get_d2l_from_db(ses):
    global d2l_db
    global d2l_db_props
    global d2l_db_obj
    sys.stderr.write('getting d2l info from DB\n')
    for dtl in ses.query(Dest2Link).all():
        d2l_db[dtl.dest] = dtl.linkid
        d2l_db_props[dtl.dest,dtl.linkid]=(dtl.dist,dtl.lastseen,dtl.lastupdated)
        d2l_db_obj[dtl.dest,dtl.linkid] = dtl

def get_link_history_from_file(link_history_fn):
    global link_history_cnt
    try: 
        if os.stat(link_history_fn).st_size == 0:
            raise FileEmptyError('file is empty')
        sys.stderr.write('reading link history file %s\n' % link_history_fn)
        LINKHIST = open(link_history_fn,'r')
    except OSError as o:
        sys.stderr.write('link history file error: %s\n' % o)
        #since without this file there is nothing to update in the DB, exit
        exit(1)
    except FileEmptyError as f:
        sys.stderr.write('link history file error: %s\n' %f)
        #since without this file there is nothing to update in the DB, exit
        #exit(1)
    except IOError as i:
        sys.stderr.write('File open failed: %s\n' % i)
        #since without this file there is nothing to update in the DB, exit
        exit(1)
    else:
        for line in LINKHIST:
            fields = line.strip().split('|')
            t_link_fa = (fields[1],fields[2])
            link_history_cnt[t_link_fa] = int(fields[4])
            t_link_nfa = (fields[0],fields[1],fields[2])
            link_history_cnt[t_link_nfa] = int(fields[4])
        LINKHIST.close()

def get_bdrmap_links_from_file(bdrmap_links_fn,ses):
    global d2l_new
    global d2l_new_props
    global link_db_obj
    global id2link
    try: 
        if os.stat(bdrmap_links_fn).st_size == 0:
            raise FileEmptyError('file is empty')
        sys.stderr.write('reading bdrmap links file %s\n' % bdrmap_links_fn)
        BDRMAP = open(bdrmap_links_fn,'r')
    except OSError as o:
        sys.stderr.write('Bdrmap file error: %s\n' % o)
        #since without this file there is nothing to update in the DB, exit
        exit(1)
    except FileEmptyError as f:
        sys.stderr.write('Bdrmap file error: %s\n' %f)
        #since without this file there is nothing to update in the DB, exit
        exit(1)
    except IOError as i:
        sys.stderr.write('File open failed: %s\n' % i)
        #since without this file there is nothing to update in the DB, exit
        exit(1)
    else:
        for line in BDRMAP:
            fields = line.strip().split('|')
            t_dest = fields[5]
            t_dist = int(fields[6])
            t_link = (fields[0],fields[1],fields[2])

            #filter the new dest2link by checking if
            #the named link has been seen in most of the previous
            #snapshots ignore entries where the link has not been
            #seen consistently
            #so if the link has not been seen consistently, we treat
            #it as if the destination has not seen any link in the
            #current snapshot. d2l_new will be empty for for this
            #destination
            if check_link_history((t_link[1],t_link[2]),HISTORY_THRESH):
                sys.stderr.write('bdrmap_links_fn: dest %s link %s majority_test passed mode fa thresh %d\n' % 
                                 (t_dest,str(t_link),HISTORY_THRESH))
                if t_link in link_db_obj:
                    sys.stderr.write('bdrmap_links_fn: %s in link_db_obj\n' % str(t_link))
                else:
                    sys.stderr.write('bdrmap_links_fn: %s not_in link_db_obj\n' % str(t_link))
                    new_l = Links(first=fields[0],
                                  second=fields[1],
                                  asn=fields[2])
                    ses.add(new_l)
                    ses.flush()
                    link_db_obj[t_link]=new_l
                    sys.stderr.write('bdrmap_links_fn: new_link_id %d\n' % 
                                     new_l.id)
                    id2link[new_l.id] = t_link
                t_link_id = link_db_obj[t_link].id
                d2l_new[t_dest] = t_link_id
                d2l_new_props[t_dest,t_link_id]=(t_dist,0,0)
            else:
                sys.stderr.write('bdrmap_links_fn: dest %s link %s majority_test failed mode fa thresh %d\n' 
                                 % (t_dest,str(t_link),HISTORY_THRESH))
        BDRMAP.close()

def get_link_history_cnt(l):
    if l not in link_history_cnt:
        t_cnt = 0
        if DEBUG: sys.stderr.write('get_link_history_cnt: %s not_in link_history_cnt cnt 0\n' % 
                                   (str(l)))
    else:
        t_cnt = link_history_cnt[l]
        if DEBUG: sys.stderr.write('get_link_history_cnt: %s in link_history_cnt cnt %d\n' % 
                                   (str(l),t_cnt))
    return t_cnt

def check_link_history(l,thresh):
    global ONEOFF
    #if this is a oneoff run, no need to check link history
    if ONEOFF:
        return True

    t_cnt = get_link_history_cnt(l)
    sys.stderr.write('check_link_history: link_history_cnt %d thresh %d\n' % (t_cnt,thresh))
    if t_cnt >= thresh:
        return True
    else:
        return False

def do_d2l_update(ses,d,l_id,ts):
    global d2l_db_obj
    #sys.stderr.write('do_d2l_update: destination %s updating d2l\n' % d)                     
    sys.stderr.write('do_d2l_update: destination %s link %d:%s dist_db %d dist_new %d\n' 
                     % (d,l_id,str(id2link[l_id]),
                        d2l_db_props[d,l_id][0],d2l_new_props[d,l_id][0]))
    d2l_db_obj[d,l_id].lastseen = ts
    if d2l_db_props[d,l_id][0] != d2l_new_props[d,l_id][0]:
        sys.stderr.write('do_d2l_update: destination %s link %d:%s updating dist\n' % 
                         (d,l_id, str(id2link[l_id])))
        d2l_db_obj[d,l_id].dist = d2l_new_props[d,l_id][0]
        d2l_db_obj[d,l_id].lastupdated = ts
    ses.flush()

def do_d2l_add(ses,d,l_id,ts):
    #sys.stderr.write('do_d2l_add: destination %s adding d2l\n' % d)
    sys.stderr.write('do_d2l_add: destination %s adding link %d:%s\n' 
                     % (d,l_id,str(id2link[l_id])))
    ins_d2l = Dest2Link(dest=d,
                        linkid=l_id,
                        dist=d2l_new_props[d,l_id][0],
                        lastseen=ts,
                        lastupdated=ts)
    ses.add(ins_d2l)
    ses.flush()

def do_d2l_replace(ses,d,l_id_old,l_id_new,ts):
    #sys.stderr.write('do_d2l_replace: destination %s replacing d2l\n' % d)                     
    sys.stderr.write('do_d2l_replace: destination %s replacing link %d:%s with link %d:%s\n' 
                     % (d,l_id_old,str(id2link[l_id_old]),
                        l_id_new,str(id2link[l_id_new])))
    ses.delete(d2l_db_obj[d,l_id_old])
    ins_d2l = Dest2Link(dest=d,
                        linkid=l_id_new,
                        dist=d2l_new_props[d,l_id_new][0],
                        lastseen=ts,
                        lastupdated=ts)
    ses.add(ins_d2l)
    ses.flush()

def do_d2l_purge(ses,d,l_id,ts):
    #sys.stderr.write('do_d2l_purge: destination %s check_purging d2l\n' % d)
    sys.stderr.write('do_d2l_purge: destination %s link %d:%s lastseen %d timestamp %d\n' 
                             % (d,l_id,id2link[l_id],d2l_db_props[d,l_id][1],ts))
    #check purge condition
    if d2l_db_props[d,l_id][1] + PURGETIME < ts:
        sys.stderr.write('do_d2l_purge: destination %s purging link %d:%s\n' 
                         % (d,l_id,str(id2link[l_id])))
        ses.delete(d2l_db_obj[d,l_id])
        ses.flush()

def main():
    db_fn=None
    bdrmap_links_fn=None
    link_history_fn=None
    timestamp=None
    global DEBUG
    global ONEOFF
    
    try:
        opts, args = getopt.getopt(sys.argv[1:], 
                                   "hdob:l:r:t:", 
                                   ["help", "debug", "oneoff",
                                    "db=", "links==", 
                                    "history=", 
                                    "timestamp="])
    except getopt.GetoptError as err:
        print str(err)
        sys.exit(1)

    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-d","--debug"):
            DEBUG=True
        elif o in ("-o","--oneoff"):
            ONEOFF=True
        elif o in ("-b","--db"):
            db_fn = a
        elif o in ("-l","--links"):
            bdrmap_links_fn = a
        elif o in ("-r","--history"):
            link_history_fn = a
        elif o in ("-t","--timestamp"):
            timestamp = int(a)
        else:
            assert False, "unhandled option"
            
    if not db_fn:
        sys.stderr.write('db file must be provided\n')
        sys.exit(1)
    
    if not bdrmap_links_fn:
        sys.stderr.write('bdrmap links file must be provided\n')
        sys.exit(1)

    if not link_history_fn and not ONEOFF:
        sys.stderr.write('link history file must be provided\n')
        sys.exit(1)

    if not timestamp:
        sys.stderr.write('timestamp must be provided\n')
        sys.exit(1)

    #connect to the sqlite database
    s = connect_db(db_fn)
    
    #get links from DB, populate id2link and link_db_obj dicts
    get_links_from_db(s)

    #get link history from the link_history_fn
    #populates the link_hist_cnt dict
    if not ONEOFF:
        get_link_history_from_file(link_history_fn)

    #get new links from bdrmap_links_fn
    #populates d2l_new and d2l_new_props
    #adds new links seen in this file to the DB if it passes majority check
    get_bdrmap_links_from_file(bdrmap_links_fn,s)    

    #get the set of d2l mappings from the DB
    #populates d2l_db, d2l_db_props and d2l_db_obj dicts
    get_d2l_from_db(s)
    
    #for each destination either in the new file or in the db
    for d in set().union(d2l_db.keys(),d2l_new.keys()):
        sys.stderr.write('----------------------------------\n')
        sys.stderr.write('destination %s: start processing\n' % d)
        if d in d2l_new and d in d2l_db:
            l_id_new = d2l_new[d]
            l_id_old = d2l_db[d]
            #if same linkid, update distance if necessary
            if l_id_new == l_id_old:
                sys.stderr.write('destination %s: d2l_db %d:%s d2l_new %d:%s update\n'
                                 % (d,l_id_old,str(id2link[l_id_old]),
                                    l_id_new,str(id2link[l_id_new])))
                do_d2l_update(s,d,l_id_old,timestamp)
            else:
                #we have already checked that the new link passed the
                #majority test, else it would not have been in d2l_new
                sys.stderr.write('destination %s: d2l_db %d:%s d2l_new %d:%s replace\n'
                                 % (d, l_id_old,str(id2link[l_id_old]),
                                    l_id_new,str(id2link[l_id_new])))
                do_d2l_replace(s,d,l_id_old,l_id_new,timestamp)
        elif d in d2l_new:
            #only new defined, just add
            #we have already checked that the new link passed the
            #majority test, else it would not have been in d2l_new
            l_id_new = d2l_new[d]
            sys.stderr.write('destination %s: d2l_new %d add\n' % (d,l_id_new))
            do_d2l_add(s,d,l_id_new,timestamp)
        elif d in d2l_db:
            #new not defined, add to purge set

            #note: we could have seen a new d2l, but it didn't pass the
            #majority test, so it's as if it's not in the set. This
            #means that the exisiting d2l won't be updated with a new
            #distance if there is one. doing that would require
            #keeping track of the distance to links that are not in
            #the bdrmap output. that's a TODO for now

            l_id_old = d2l_db[d]
            sys.stderr.write('destination %s: d2l_db %d purge\n' % (d,l_id_old))
            do_d2l_purge(s,d,l_id_old,timestamp)

    s.commit()        
    exit(0)

if __name__ == "__main__":
    main()
