#checks a bdrmap warts file to see if there are at least a threshold
#number of "trace" objects.
#the threshold is an input parameter that defaults to 500000

import sys
import re
import json
from subprocess import PIPE, Popen
from collections import defaultdict
import getopt
import os
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

if __name__ == "__main__":

    DEBUG=0
    warts_fn=None
    count_trace=0

    #default trace_threshold
    trace_thresh=500000

    try:
        opts, args = getopt.getopt(sys.argv[1:], "hdw:t:", ["help", "debug"])
    except getopt.GetoptError as err:
        print str(err) 
        sys.exit(2)

    for o, a in opts:
        if o in ("-h", "--help"):
            sys.exit(2)
        elif o in ("-d","--debug"):
            DEBUG=1
        elif o in ("-w","--warts"):
            warts_fn = a
        elif o in ("-t","--tracethresh"):
            trace_thresh = int(a)
        else:
            assert False, "unhandled option"

    if not warts_fn:
        sys.stderr.write('check_bdrmap_warts: warts file must be provided\n')
        sys.exit(2)

    try: 
        if os.stat(warts_fn).st_size == 0:
            raise FileEmptyError('file is empty')
        sys.stderr.write('check_bdrmap_warts: reading warts %s\n' % warts_fn)
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
        sys.stderr.write('check_bdrmap_warts: warts file error: %s\n' % o)
        sys.exit(1)
    except FileEmptyError as f:
        sys.stderr.write('check_brmap_warts: warts file error: %s\n' %f)
        sys.exit(1)
    except IOError as i:
        sys.stderr.write('check_bdrmap_warts: File open failed: %s\n' % i)
        sys.exit(1)
    else:
        for line in jsonizer.stdout:
            parsed_json = json.loads(line.strip())
            if parsed_json['type'] != "trace": continue
            count_trace += 1

    if count_trace > trace_thresh:
        sys.stderr.write('check_bdrmap_warts: warts_file %s trace_thresh %d count_trace %d success\n' % (warts_fn,trace_thresh,count_trace))
        sys.exit(0)
    else:
        sys.stderr.write('check_bdrmap_warts: warts_file %s trace_thresh %d count_trace %d fail\n' % (warts_fn,trace_thresh,count_trace))
        sys.exit(1)
