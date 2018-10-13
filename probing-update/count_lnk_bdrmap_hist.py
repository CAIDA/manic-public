import sys
from collections import defaultdict
import getopt
import os
import re
from myexceptions import *
import bz2
import glob
from sets import Set

#set of timestamps (bdrmap warts files) in which 
#each link was seen
linksnaps = defaultdict(Set)

def get_file_ts(fn):
    fields = fn.split(".")
    ts = fields[-3]
    return int(ts)

def main():
    DEBUG=0
    history_dir=None
    #default
    num_snaps=5

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hdl:s:", ["help", "debug", "linkdir=", "snaps="])
    except getopt.GetoptError as err:
        print str(err)
        sys.exit(1)

    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-d","--debug"):
            DEBUG=1
        elif o in ("-l","--linkdir"):
            history_dir = a
        elif o in ("-s","--snaps"):
            num_snaps = int(a)
        else:
            assert False, "unhandled option"

    if not history_dir:
        sys.stderr.write('link history directory file must be provided\n')
        sys.exit(1)

    link_files = []
    glob_pattern = history_dir + '/' + '*lnk*.out.bz2'
    files = glob.glob(glob_pattern)
    files.sort(key=get_file_ts, reverse=True)
    if DEBUG: print files

    files = files[:num_snaps]
    # if len(files) < num_snaps:
    #     sys.stderr.write('not enough snapshots to create link visibilty history\n')
    #     sys.exit(1)
    
    for link_fn in files:
        try:
            if os.stat(link_fn).st_size == 0:
                raise FileEmptyError('file is empty')
            sys.stderr.write('reading link fn %s\n' % link_fn)
            file_ts = get_file_ts(link_fn)
            LNK = bz2.BZ2File(link_fn,'r')
        except OSError as o:
            sys.stderr.write('link_fn error: %s\n' % o)
            #sys.exit(1)
        except FileEmptyError as f:
            sys.stderr.write('link_fn error: %s\n' %f)
            #sys.exit(1)
        except IOError as i:
            sys.stderr.write('opening link_fn failed: %s\n' % i)
            #sys.exit(1)
        else:
            for line in LNK:
                if re.search('^#',line): continue
                fields = line.strip().split("|")
                #count link based on far IP and nbr ASN
                t_lnk_fa = (fields[1],fields[2])
                #count link based on near IP, far IP and nbr ASN
                t_lnk_nfa = (fields[0],fields[1],fields[2])
                linksnaps[t_lnk_fa].add(file_ts)
                linksnaps[t_lnk_nfa].add(file_ts)
            LNK.close()

    for l in linksnaps:
        #(near, far, asn)
        if len(l) == 3:
            sys.stdout.write("nfa|%s|%s|%s|%d|%d\n" % (l[0],l[1],l[2],num_snaps,len(linksnaps[l])))
        #(far, asn)
        if len(l) == 2:
            sys.stdout.write("fa|%s|%s|%d|%d\n" % (l[0],l[1],num_snaps,len(linksnaps[l])))

    sys.exit(0)

if __name__ == "__main__":
    main()
