#!/usr/local/bin/bash

. config.bash

MON=$1

echo "make sure $MON.sibling.txt links to the correct sibling file in $sibdir";
echo "make sure $MON.csum.txt has a csum value in $csumdir";
echo "creating directories in ${mondatadirbase}/$MON";

mkdir ${mondatadirbase}/${MON};
mkdir ${mondatadirbase}/${MON}/bdrmap-state;

mondatadir="${mondatadirbase}/${MON}/bdrmap-state";

mkdir ${mondatadir}/warts;
mkdir ${mondatadir}/warts/current;
mkdir ${mondatadir}/warts/archive;
mkdir ${mondatadir}/warts/logs;
mkdir ${mondatadir}/history;
mkdir ${mondatadir}/logs;

echo "creating mon DB\n";
python ${codedir}/create_db_lnk.py ${mondatadir}/${MON}.monstate.lnk.db;

init_ts="0000000000";

touch ${mondatadir}/${MON}.dot1.max3.${init_ts}.targets
ln -s -f ${mondatadir}/${MON}.dot1.max3.${init_ts}.targets ${mondatadir}/${MON}.dot1.max3.targets

touch ${mondatadir}/${MON}.scattach.${init_ts}.targets
ln -s -f ${mondatadir}/${MON}.scattach.${init_ts}.targets ${mondatadir}/${MON}.scattach.targets

touch ${mondatadir}/${MON}.link2dest.max3.${init_ts}.out
ln -s -f ${mondatadir}/${MON}.link2dest.max3.${init_ts}.out ${mondatadir}/${MON}.link2dest.max3.out

touch ${mondatadir}/${MON}.link2dest.${init_ts}.raw
ln -s -f ${mondatadir}/${MON}.link2dest.${init_ts}.raw ${mondatadir}/${MON}.link2dest.raw

touch ${mondatadir}/${MON}.lnk.${init_ts}.out
ln -s -f ${mondatadir}/${MON}.lnk.${init_ts}.out ${mondatadir}/${MON}.lnk.out

touch ${mondatadir}/${MON}.bdrmap.${init_ts}.out
ln -s -f ${mondatadir}/${MON}.bdrmap.${init_ts}.out ${mondatadir}/${MON}.bdrmap.out

touch ${mondatadir}/${MON}.aliases.${init_ts}.out
ln -s -f ${mondatadir}/${MON}.aliases.${init_ts}.out ${mondatadir}/${MON}.aliases.out

echo ${init_ts} > ${mondatadir}/${MON}.last_update_ts.txt
