import sys
import re
import json
from subprocess import PIPE, Popen
from collections import defaultdict
import getopt
import os
import bdrmap_parse
from myexceptions import *

def decode_trace(json_obj):
    trace={} 
    trace['userid'] = json_obj['userid']
    trace['hopc'] = 0;
    trace['src'] = json_obj['src']
    trace['dst'] = json_obj['dst']
    trace['start'] = json_obj['start']['sec']
    trace['hops']={}
    
    if 'hops' not in json_obj:
        return None

    for hop in json_obj['hops']:
        ttl = hop['probe_ttl']
        trace['hopc'] = ttl
        if ttl not in trace['hops']:
            trace['hops'][ttl] = hop
  
    return trace

def do_aliases(router):
    global aliases
    for i1 in router.interfaces:
        if i1.ip not in aliases: aliases[i1.ip] = defaultdict(dict)
        for i2 in router.interfaces:
            if i2.ip not in aliases: aliases[i2.ip] = defaultdict(dict)
            if i1.ip != i2.ip:
                aliases[i1.ip][i2.ip]=1;
                aliases[i2.ip][i1.ip]=1;                

def dump_group(router):
    global linkinfo
    global interfaces
    global obs_group
    global siblings
    if DEBUG:
        sys.stderr.write('router owner %s\n' % router.owner)
        sys.stderr.write('router rel %s\n' % router.rel)
        sys.stderr.write('router reason %s\n' % router.reason)
        
    if int(router.owner) == 0: return
    if int(router.owner) not in siblings: return
    if DEBUG: sys.stderr.write('processing router owner %s\n' % router.owner)
    do_aliases(router)
    near_stars=0
    for n_if in router.interfaces:
        if n_if.is_star(): 
            near_stars+=1
    for n_if in router.interfaces:
        if DEBUG: sys.stderr.write('near_if %s\n' % n_if)
        for f_r in router.neighbors:
            do_aliases(f_r)
            if int(f_r.owner) == '0': continue
            #shouldn't be any with rel self but check anyway
            if f_r.rel == 'self': continue
            far_stars=0
            for f_if in f_r.interfaces:
                if f_if.is_star(): 
                    far_stars+=1
            for f_if in f_r.interfaces:
                linkinfo[(n_if.ip,f_if.ip)] = (int(f_r.owner),f_r.rel,f_r.reason)
                interfaces[n_if.ip]=1
                interfaces[f_if.ip]=1
                if f_if.is_star() and n_if.is_star():
                    linkinfostar[(n_if.ip,f_if.ip)] = (int(f_r.owner),f_r.rel,f_r.reason)
                    if near_stars==1 and far_stars==1:
                        linkinfostarexclusive[(n_if.ip,f_if.ip)] = 1

            #should see at least one link from this near,far grp
            if near_stars >0 and far_stars > 0:
                obs_group[(':'.join(map(lambda x: x.ip, r.interfaces)),':'.join(map(lambda x: x.ip,f_r.interfaces)))]=1

siblings={}
aliases={}
linkinfo = {}
linkinfostar = {}
linkinfostarexclusive = {}
obs_group={}
interfaces = {}

if __name__ == "__main__":
    DEBUG=0
    alias_fn=None
    links_fn=None
    warts_fn=None
    bdrmap_fn=None
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hda:l:b:w:s:", ["help", "debug"])
    except getopt.GetoptError as err:
        print str(err) 
        #usage()
        sys.exit(2)

    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-d","--debug"):
            DEBUG=1
        elif o in ("-b","--bdrmap"):
            bdrmap_fn = a
        elif o in ("-w","--warts"):
            warts_fn = a
        elif o in ("-s", "--siblings"):
            sibling_fn = a
        elif o in ("-a","--aliasout"):
            alias_fn=a
        elif o in ("-l","--linksout"):
            links_fn=a        
        else:
            assert False, "unhandled option"

    if not bdrmap_fn:
        sys.stderr.write('bordermap file must be provided\n')
        sys.exit(2)

    if not warts_fn:
        sys.stderr.write('warts file must be provided\n')
        sys.exit(2)

    if not sibling_fn:
        sys.stderr.write('siblings file must be provided\n')
        sys.exit(2)

    if alias_fn is None:
        alias_fn = '.'.join(bdrmap_fn.split('.')[:-2])+".aliases.out"
    print "alias_fn",alias_fn
    aliasfh = open(alias_fn,"w")

    if links_fn is None:
        links_fn = '.'.join(bdrmap_fn.split('.')[:-2])+".links.out"
    print "links_fn",links_fn
    linksfh = open(links_fn,"w")

    try:
        if os.stat(sibling_fn).st_size == 0:
            raise FileEmptyError('file is empty')
        sys.stderr.write('reading sibling list %s\n' % sibling_fn)
        SIB = open(sibling_fn,'r')
    except OSError as o:
        sys.stderr.write('sibling list error: %s\n' % o)
        sys.exit(1)
    except FileEmptyError as f:
        sys.stderr.write('sibling list error: %s\n' %f)
        sys.exit()
    except IOError as i:
        sys.stderr.write('sibling list failed: %s\n' % i)
        sys.exit(1)
    else:
        for line in SIB:
            if re.search('^#',line): continue
            fields = line.strip().split()
            siblings[int(fields[0])]=1
        SIB.close()

    if DEBUG:
        for s in siblings:
            sys.stderr.write('sibling %d\n' % s)
        
    try: 
        (routers,inf2rtr,inf2name) = bdrmap_parse.read_bdrmap_file(bdrmap_fn)
    except OSError as o:
        sys.stderr.write('bordermap file error: %s\n' % o)
        sys.exit(1)
    except FileEmptyError as f:
        sys.stderr.write('bordermap file error: %s\n' %f)
        sys.exit(1)
    except IOError as i:
        sys.stderr.write('File open failed: %s\n' % i)
        sys.exit(1)

    for r in routers:
        dump_group(r)

    intf_to_dest={}
    for i in interfaces:
        intf_to_dest[i] = defaultdict(dict)

    link_to_dest={}
    for l in linkinfo:
        link_to_dest[l] = defaultdict(dict)

    try: 
        if os.stat(warts_fn).st_size == 0:
            raise FileEmptyError('file is empty')
        sys.stderr.write('reading warts %s\n' % warts_fn)
        if re.search('\.warts$',warts_fn):
            jsonizer = Popen(['/home/amogh/software/scamper/bin/sc_warts2json', warts_fn],stdout=PIPE)
        else:
            if re.search('\.gz$',warts_fn):
                zcat = Popen(['zcat',warts_fn],stdout=PIPE)
            elif re.search('\.bz2$',warts_fn):
                zcat = Popen(['bzcat',warts_fn],stdout=PIPE)
            jsonizer = Popen(['/home/amogh/software/scamper/bin/sc_warts2json'],stdin=zcat.stdout,stdout=PIPE)
            zcat.stdout.close()
    except OSError as o:
        sys.stderr.write('warts file error: %s\n' % o)
        sys.exit(1)
    except FileEmptyError as f:
        #since without this file there is nothing more to update in the DB, exit
        sys.stderr.write('warts file error: %s\n' %f)
        sys.exit(1)
    except IOError as i:
        #since without this file there is nothing more to update in the DB, exit
        sys.stderr.write('File open failed: %s\n' % i)
        sys.exit(1)
    else:
        for line in jsonizer.stdout:
            parsed_json = json.loads(line.strip())
            if parsed_json['type'] != "trace": continue
            t_trace = decode_trace(parsed_json)
            if t_trace is None: continue
            n_hops = t_trace['hopc']
            if 'hops' not in t_trace: continue
            #print t_trace
            for i in range(1,n_hops):
                if i not in t_trace['hops']: continue
                if i+1 not in t_trace['hops']: continue
                t_intf = t_trace['hops'][i]['addr']
                tnext_intf = t_trace['hops'][i+1]['addr']
                if DEBUG: sys.stderr.write('checking link %s,%s\n' %(t_intf,tnext_intf))
                if (t_intf,tnext_intf) in linkinfostar:
                    #shouldn't really be in linkinfo if rel is self but
                    #you never know what can happen, so check anyway
                    if linkinfostar[(t_intf,tnext_intf)][1] == 'self':
                        #sys.stderr.write('link %s,%s in linkinfo but rel is self. skipping.\n'
                        #%(t_intf,tnext_intf))
                        continue
                    else:
                        if DEBUG: sys.stderr.write('link %s,%s in linkinfostar, rel is %s. breaking.\n'
                                                   %(t_intf,tnext_intf,linkinfostar[(t_intf,tnext_intf)][1]))
                        link_to_dest[(t_intf,tnext_intf)][t_trace['dst']] = i
                        #break on seeing the first identified link
                        break
        #jsonizer.close()

    for link in link_to_dest:
        (owner, rel, reason) = linkinfo[link]
        for dest in link_to_dest[link]:
            linksfh.write('%s|%s|%s|%s|%s|%s|%d\n' % 
                            (link[0], link[1], owner, rel, reason,dest,link_to_dest[link][dest]))
    linksfh.close()

    if DEBUG:
        n_notseen=0
        for link in linkinfo:
            t_star=0
            t_starex=0
            if len(link_to_dest[link].keys()) > 0: continue
            sys.stderr.write('link %s %s: no destination found\n' % (link[0],link[1]))
            n_notseen+=1
            if link in linkinfostar:
                t_star=1
                if link in linkinfostarexclusive:
                    t_starex=1
            sys.stderr.write('link %s %s: star: %d starex: %d num_dest %d\n'
                             %(link[0],link[1],t_star,t_starex, len(link_to_dest[link].keys()))) 
            
            n_t = len(linkinfo.keys())
            n_t_star = len(linkinfostar.keys())
            sys.stderr.write('links with no destinations:%d\n' % n_notseen)
            sys.stderr.write('links total:%d\n' % n_t)
            sys.stderr.write('links total star:%d\n' % n_t_star)

            for (near,far) in obs_group:
                near_ips = near.split(':')
                far_ips = far.split(':')
                atleast1=0;
                for n_ip in near_ips:
                    n_nostar = re.sub('\*','',n_ip)
                    for f_ip in far_ips:
                        f_nostar = re.sub('\*','',f_ip)
                        if (n_nostar,f_nostar) in link_to_dest:
                            atleast1=1
                if atleast1:
                    sys.stderr.write('rtr pair %s %s at least one seen\n' % (near,far) )
                else:
                    sys.stderr.write('hmmm, rtr pair %s %s nothing seen\n' % (near,far) )

    trclose={};
    grouped={};

    sys.stderr.write('starting aliases\n')
    for i1 in aliases:
        if i1 in grouped: continue
        q = aliases[i1].keys()
        while len(q) != 0: 
            i2 = q.pop()
            if i2 in grouped or i2 == i1: continue
            if i1 not in trclose: trclose[i1] = defaultdict(dict)
            trclose[i1][i2]=1;
            grouped[i2]=1;
            for i in aliases[i2]:
                q.append(i2)
                grouped[i1]=1

    for i1 in trclose:
        #print i1
        aliasfh.write('%s %s\n' % (i1, ' '.join(trclose[i1].keys())))
    aliasfh.close()

    sys.exit(0)
