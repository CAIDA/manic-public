import sys
import re
import os
import getopt
from sets import Set
from  myexceptions import *

def get_ip_nostar(ip):
    ip_nostar = re.sub('\*','',ip)
    return ip_nostar

def is_ip_star(ip):
    if re.search('\*',ip):
        return 1
    else:
        return 0

def is_owner_line(line):
    m = re.search("^owner (\d+)", line)
    if m:
        return 1
    else:
        return 0

def is_ip_line(line):
    m = re.search("^(\d+\.\d+\.\d+\.\d+\**)", line)
    if m:
        return 1
    else:
        return 0

def is_nbr_rtr_line(line):
    m = re.search("^ (\d+) .+? .+? (.+)", line)
    if m:
        return 1
    else:
        return 0

def is_name(line):
    m = re.search("\.", line)
    if m:
        return 1
    else:
        return 0
    
def ip_sort_fn(ip):
    octets = ip.split(".")
    return int(octets[0])

class Interface:
    def __init__(self, ip, star):
        self.ip = ip
        self.star = star
        self.name = None

    def set_name(self,name):
        self.name = name

    def set_star(self,star):
        self.star = star

    def get_addr(self):
        return self.ip

    def get_name(self):
        if self.name is None:
            return ""
        else:
            return self.name

    def is_star(self):
        if self.star:
            return 1
        else:
            return 0

class Router:
    def __init__(self, router_id):
        self.id = router_id
        self.interfaces = Set()
        self.owner = None
        self.rel = None
        self.reason = None
        self.neighbors = Set()

    def add_interface(self, i):
        self.interfaces.add(i)

    def set_owner(self, o):
        self.owner = o

    def set_rel(self, r):
        self.rel = r

    def set_reason(self, r):
        self.reason = r

    def add_neighbor(self,n):
        self.neighbors.add(n)

def read_bdrmap_file(bdrmap_out_fn):
    try: 
        if os.stat(bdrmap_out_fn).st_size == 0:
            raise FileEmptyError('file is empty')
        sys.stderr.write('bordermap file: %s\n' % bdrmap_out_fn)
        BDRMAP = open(bdrmap_out_fn,'r')
    except OSError as o:
        sys.stderr.write('bordermap file error: %s\n' % o)
        raise
    except FileEmptyError as f:
        sys.stderr.write('bordermap file error: %s\n' %f)
        raise
    except IOError as i:
        sys.stderr.write('File open failed: %s\n' % i)
        raise
    else:
        routers = []
        interface2rtr = {}
        interface2name = {}
        router_id = -1
        for line in BDRMAP:
            #sys.stderr.write('%s\n' % line)
            fields = line.strip().split()
            if len(fields) == 0: continue
            #if fields[0] == "owner":
            if is_owner_line(line):
                far_rtrs = []
                router_id += 1
                near_rtr = Router(router_id)
                #near_rtr.set_owner(fields[1])
                near_rtr.set_owner(fields[1])
                near_rtr.set_rel('self')
                near_rtr.set_reason('self')
                routers.append(near_rtr)
            elif is_ip_line(line):
                i = Interface(get_ip_nostar(fields[0]), is_ip_star(fields[0]))
                #i = Interface(get_ip_nostar(fields[0]), is_ip_star(fields[0]))
                if len(fields) > 1 and is_name(fields[1]):
                    i.set_name(fields[1])
                    interface2name[i.ip] = fields[1]
                near_rtr.add_interface(i)
                if i.ip not in interface2rtr:
                    interface2rtr[i.ip] = near_rtr.id
            elif is_nbr_rtr_line(line):
                router_id += 1
                t_far_rtr = Router(router_id)
                t_far_rtr.set_owner(fields[0])
                t_far_rtr.set_rel(fields[1])
                t_far_rtr.set_reason(fields[2])
                for f in range(3,len(fields)):
                    if is_ip_line(fields[f]): #this is an ip address
                        i = Interface(get_ip_nostar(fields[f]), is_ip_star(fields[f]))
                        if f+1 < len(fields) and not is_ip_line(fields[f+1]) and is_name(fields[f+1]):
                            i.set_name(fields[f+1])
                            interface2name[i.ip] = fields[f+1]
                        t_far_rtr.add_interface(i)
                        if i.ip not in interface2rtr:
                            interface2rtr[i.ip] = t_far_rtr.id
                routers.append(t_far_rtr)
                near_rtr.add_neighbor(t_far_rtr)
                    
        return (routers,interface2rtr,interface2name)

if __name__ == "__main__":
    bdrmap_fn=None
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hdb:n:", ["help", "debug"])
    except getopt.GetoptError as err:
        print str(err) 
        usage()
        sys.exit(2)

    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-d","--debug"):
            DEBUG=1
        elif o in ("-b","--bdrmap"):
            bdrmap_fn = a
        else:
            assert False, "unhandled option"

    if not bdrmap_fn:
        sys.stderr.write('bordermap file must be provided\n')
        sys.exit(2)    

    (routers,interface2rtr,interface2name) = read_bdrmap_file(bdrmap_fn)
    count=0
    for r in routers:
        print count, r.id, r.owner
        for i in r.interfaces:
            print " "+str(i.ip)+" "+str(i.star)+" "+i.get_name()
        for f in r.neighbors:
            print "    ", f.owner,f.rel,f.reason,",".join(map(Interface.get_addr, f.interfaces))
        print
        count += 1

    for i in interface2rtr:
        print "interface", i, "rtr", interface2rtr[i]

    for i in interface2name:
        print "interface", i, "name", interface2name[i]
