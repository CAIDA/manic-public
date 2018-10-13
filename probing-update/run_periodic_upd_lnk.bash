#!/usr/local/bin/bash

error_exit () {
    if [ $1 -ne 0 ] 
    then
	echo "$2 errcode $1"
	#rollback
	#compress_warts
	#compress_logs
	exit 1
    fi
}

error_exit_norb () {
    if [ $1 -ne 0 ] 
    then
	echo "$2 errcode $1"
	exit 1
    fi
}

#rolls back state in case a critical step fails
rollback () {
    echo "$MON:$ts: rolling back state"
    
    echo "$MON:$ts: copying ${mondatadir}/${MON}.monstate.lnk.db.bak to ${mondatadir}/${MON}.monstate.lnk.db"
    cp ${mondatadir}/${MON}.monstate.lnk.db.bak ${mondatadir}/${MON}.monstate.lnk.db
    echo "removing lock file ${mondatadir}/$MON.db-lock";
    rm -f ${mondatadir}/${MON}.db-lock;

    echo "$MON:$ts: restoring ${mondatadir}/history/${prev_targets_fn_base} to ${mondatadir}/"
    mv ${mondatadir}/history/${prev_targets_fn_base} ${mondatadir}/
    ln -s -f ${mondatadir}/${prev_targets_fn_base} ${mondatadir}/${MON}.dot1.max3.targets
    
    echo "$MON:$ts: restoring ${mondatadir}/history/${prev_scattach_fn_base} to ${mondatadir}/"
    mv ${mondatadir}/history/${prev_scattach_fn_base} ${mondatadir}/
    ln -s -f ${mondatadir}/${prev_scattach_fn_base} ${mondatadir}/${MON}.scattach.targets

    echo "$MON:$ts: restoring ${mondatadir}/history/${prev_link2dest_fn_base} to ${mondatadir}/"
    mv ${mondatadir}/history/${prev_link2dest_fn_base} ${mondatadir}/
    ln -s -f ${mondatadir}/${prev_link2dest_fn_base} ${mondatadir}/${MON}.link2dest.max3.out
    
    echo "$MON:$ts: restoring ${mondatadir}/history/${prev_lnkout_fn_base} to ${mondatadir}/"
    mv ${mondatadir}/history/${prev_lnkout_fn_base}.bz2 ${mondatadir}/
    bunzip2 ${mondatadir}/${prev_lnkout_fn_base}.bz2
    ln -s -f ${mondatadir}/${prev_lnkout_fn_base} ${mondatadir}/${MON}.lnk.out

    echo "$MON:$ts restoring ${mondatadir}/history/${prev_aliasout_fn_base} to ${mondatadir}/"
    mv ${mondatadir}/history/${prev_aliasout_fn_base} ${mondatadir}/
    ln -s -f ${mondatadir}/${prev_aliasout_fn_base} ${mondatadir}/${MON}.aliases.out

    echo "$MON:$ts restoring ${mondatadir}/history/${prev_bdrmapout_fn_base} to ${mondatadir}/"
    mv ${mondatadir}/history/${prev_bdrmapout_fn_base} ${mondatadir}/
    ln -s -f ${mondatadir}/${prev_bdrmapout_fn_base} ${mondatadir}/${MON}.bdrmap.out

    echo "$MON:$ts: restoring ${mondatadir}/history/${prev_link2destraw_fn_base} to ${mondatadir}/"
    mv ${mondatadir}/history/${prev_link2destraw_fn_base} ${mondatadir}/
    ln -s -f ${mondatadir}/${prev_link2destraw_fn_base} ${mondatadir}/${MON}.link2dest.raw
    
    rm -f ${mondatadir}/${MON}.bdrmap.${ts}.out
    rm -f ${mondatadir}/${MON}.aliases.${ts}.out
    rm -f ${mondatadir}/${MON}.lnk.${ts}.out
    rm -f ${mondatadir}/${MON}.link2dest.${ts}.raw
    rm -f ${mondatadir}/${MON}.lnkhist.cnt.${ts}.txt

    rm -f ${mondatadir}/${MON}.link2dest.max3.${ts}.out
    rm -f ${mondatadir}/${MON}.link2dest.max3.out.prev

    rm -f ${mondatadir}/${MON}.dot1.max3.${ts}.targets
    rm -f ${mondatadir}/${MON}.scattach.${ts}.targets
    rm -f ${memfsdir}/${MON}.monstate.lnk.db
}

compress_warts() {
    echo "$MON:$ts: compressing warts file.."
    gzip $fnoext
}

compress_backups() {
    echo "$MON:$ts: finished, compressing files moved to history.."
    bzip2 -9 ${mondatadir}/history/${prev_aliasout_fn_base}
    bzip2 -9 ${mondatadir}/history/${prev_bdrmapout_fn_base}
    bzip2 -9 ${mondatadir}/history/${prev_link2destraw_fn_base}
    bzip2 -9 ${mondatadir}/history/${prev_targets_fn_base}
    bzip2 -9 ${mondatadir}/history/${prev_scattach_fn_base}
    if [ $ONEOFF -eq "0" ]; then
	bzip2 -9 ${mondatadir}/history/${MON}.lnkhist.cnt.${ts}.txt
    fi

    bzip2 -9 ${mondatadir}/history/${prev_link2dest_fn_base}
    rm -f ${mondatadir}/${MON}.link2dest.max3.out.prev
}

compress_logs() {
    echo "$MON:$ts: compressing log files.."
    bzip2 -9 -f ${mondatadir}/logs/${MON}.ext.${ts}.log
    bzip2 -9 -f ${mondatadir}/logs/${MON}.get.${ts}.log
    bzip2 -9 -f ${mondatadir}/logs/${MON}.meta_lnk.${ts}.log
    bzip2 -9 -f ${mondatadir}/logs/${MON}.sel_lnk.${ts}.log
    bzip2 -9 -f ${mondatadir}/logs/${MON}.upd.${ts}.log
    bzip2 -9 -f ${mondatadir}/logs/${MON}.upd_alias.${ts}.log
}

. config.bash

#name of the monitor to process 
MON=$1
#1 for single-shot run, 0 for continuous updates
ONEOFF=$2

CSUM=`cat $csumdir/$MON.csum.txt`
SIB=$sibdir/$MON.sibling.txt
wallts=`date +%s`

echo $MON
echo $CSUM
echo $SIB

#the directory where probing state for this monitor is stored
mondatadir="${mondatadirbase}/${MON}/bdrmap-state"
echo ${mondatadir};

last_update_ts=`cat ${mondatadir}/${MON}.last_update_ts.txt`
echo "$MON: last update_ts $last_update_ts"

#get the latest warts file that is after last_update_ts
f=`${codedir}/get_warts_bdrmap.pl ${mondatadir}/warts/current ${last_update_ts}`

#if no warts file, nothing to do
echo "$MON:$ts: warts file: $f"
if [ "$f" = "" ]
then
    echo "$MON:$ts no warts file. Exiting.";
    exit 1
fi

#check if bdrmap warts file has at least a mininum number of traces, set to 500000
python ${codedir}/check_bdrmap_warts.py -w $f -t 500000
error_exit_norb $? "$MON:$ts: bdrmap warts check failed."

gunzip $f
ts=`echo $f | cut -d "." -f2`
echo $ts

fnoext=${f%.*}
echo $fnoext

echo "creating lock file ${mondatadir}/$MON.db-lock";
touch ${mondatadir}/${MON}.db-lock;

#the probing update process using the sqlite DB runs faster if we use a memory FS
echo "$MON:$ts: copying monstate DB to mfs"
cp ${mondatadir}/${MON}.monstate.lnk.db ${memfsdir}

echo "$MON:$ts: copying monstate DB ${mondatadir}/${MON}.monstate.lnk.db to ${mondatadir}/${MON}.monstate.lnk.db.bak"
cp ${mondatadir}/${MON}.monstate.lnk.db ${mondatadir}/${MON}.monstate.lnk.db.bak 
error_exit $? "$MON:$ts: copy ${mondatadir}/${MON}.monstate.lnk.db to .bak failed."

echo "$MON:$ts: copying max3.targets to history"
prev_targets_fn=`readlink ${mondatadir}/${MON}.dot1.max3.targets`
prev_targets_fn_base=`basename $prev_targets_fn`
mv $prev_targets_fn ${mondatadir}/history/
error_exit $? "$MON:$ts: copy ${prev_targets_fn_base} to ${mondatadir}/history/ failed."

echo "$MON:$ts: copying scattach.targets to history"
prev_scattach_fn=`readlink ${mondatadir}/${MON}.scattach.targets`
prev_scattach_fn_base=`basename $prev_scattach_fn`
mv $prev_scattach_fn ${mondatadir}/history/
error_exit $? "$MON:$ts: copy ${prev_scattach_fn_base} to ${mondatadir}/history/ failed."

echo "$MON:$ts: copying link2dest.max3.out to history"
prev_link2dest_fn=`readlink ${mondatadir}/${MON}.link2dest.max3.out`
prev_link2dest_fn_base=`basename ${prev_link2dest_fn}`
mv ${prev_link2dest_fn} ${mondatadir}/history/
ln -s -f ${mondatadir}/history/${prev_link2dest_fn_base} ${mondatadir}/${MON}.link2dest.max3.out.prev

echo "$MON:$ts: copying lnk.out to history"
prev_lnkout_fn=`readlink ${mondatadir}/${MON}.lnk.out`
prev_lnkout_fn_base=`basename ${prev_lnkout_fn}`
mv ${prev_lnkout_fn} ${mondatadir}/history/
#compressing the previous lnk out file now because it is needed for computing link history in continuous mode
bzip2 -9 -f ${mondatadir}/history/${prev_lnkout_fn_base}
ls -l ${mondatadir}/history/${prev_lnkout_fn_base}.bz2
error_exit $? "$MON:$ts: copy ${prev_lnkout_fn_base} to ${mondatadir}/history/ failed."

echo "$MON:$ts: copying aliases.out to history"
prev_aliasout_fn=`readlink ${mondatadir}/${MON}.aliases.out`
prev_aliasout_fn_base=`basename ${prev_aliasout_fn}`
mv ${prev_aliasout_fn} ${mondatadir}/history/
error_exit $? "$MON:$ts: copy ${prev_aliasout_fn_base} to ${mondatadir}/history/ failed. Exiting."

echo "$MON:$ts: copying bdrmap.out to history"
prev_bdrmapout_fn=`readlink ${mondatadir}/${MON}.bdrmap.out`
prev_bdrmapout_fn_base=`basename ${prev_bdrmapout_fn}`
mv ${prev_bdrmapout_fn} ${mondatadir}/history/
error_exit $? "$MON:$ts: copy ${prev_bdrmapout_fn_base} to ${mondatadir}/history/ failed."

echo "$MON:$ts: copying link2dest.raw to history"
prev_link2destraw_fn=`readlink ${mondatadir}/${MON}.link2dest.raw`
prev_link2destraw_fn_base=`basename ${prev_link2destraw_fn}`
mv ${prev_link2destraw_fn} ${mondatadir}/history/
error_exit $? "$MON:$ts: copy ${prev_link2destraw_fn_base} to ${mondatadir}/history/ failed."

echo "$MON:$ts: running bdrmap"
$bdrmap -a ${pfx2asdir}/${pfx2asfile} -r ${pfx2asdir}/${asrel} -x ${pfx2asdir}/${peering} -g ${delegateddir}/${delegatedfile} -v ${SIB} -d 4 ${fnoext} > ${mondatadir}/${MON}.bdrmap.${ts}.out
error_exit $? "$MON:$ts: bdrmap failed. Exiting."

echo "$MON:$ts: extracting links and destinations from bordermap warts"
python ${codedir}/get_rtr_links_bdrmap.py -b ${mondatadir}/${MON}.bdrmap.${ts}.out -w ${fnoext} -s ${sibdir}/${MON}.sibling.txt -a ${mondatadir}/${MON}.aliases.${ts}.out -l ${mondatadir}/${MON}.lnk.${ts}.out > ${mondatadir}/logs/${MON}.get.${ts}.log 2>&1
error_exit $? "$MON:$ts: extracting links and destinations failed."

ln -s -f ${mondatadir}/${MON}.lnk.${ts}.out ${mondatadir}/${MON}.lnk.out
ln -s -f ${mondatadir}/${MON}.aliases.${ts}.out ${mondatadir}/${MON}.aliases.out
ln -s -f ${mondatadir}/${MON}.bdrmap.${ts}.out ${mondatadir}/${MON}.bdrmap.out

#produce link history file. only if we are running in continuous mode
if [ $ONEOFF -eq "0" ]; then
    python ${codedir}/count_lnk_bdrmap_hist.py --linkdir=${mondatadir}/history > ${mondatadir}/${MON}.lnkhist.cnt.${ts}.txt
fi

echo "$MON:$ts: updating monstate DB with link info"
if [ $ONEOFF -eq "1" ]; then
    python ${codedir}/update_db_monstate_lnk_hist.py -o -b ${memfsdir}/${MON}.monstate.lnk.db -l ${mondatadir}/${MON}.lnk.out -t ${ts} > ${mondatadir}/logs/${MON}.upd.${ts}.log 2>&1
    error_exit $? "$MON:$ts: updating monstate DB failed. Exiting."
else
    python ${codedir}/update_db_monstate_lnk_hist.py -b ${memfsdir}/${MON}.monstate.lnk.db -l ${mondatadir}/${MON}.lnk.out -r ${mondatadir}/${MON}.lnkhist.cnt.${ts}.txt -t ${ts} > ${mondatadir}/logs/${MON}.upd.${ts}.log 2>&1
    error_exit $? "$MON:$ts: updating monstate DB failed. Exiting."
fi

echo "$MON:$ts: updating monstate DB with alias info"
python ${codedir}/update_db_aliases_lnk.py ${memfsdir}/${MON}.monstate.lnk.db ${mondatadir}/${MON}.aliases.out ${ts} > ${mondatadir}/logs/${MON}.upd_alias.${ts}.log 2>&1

echo "$MON:$ts: extracting raw targets"
python ${codedir}/extract_targets_lnk.py -l -o ${mondatadir}/${MON}.link2dest.${ts}.raw ${memfsdir}/${MON}.monstate.lnk.db ${CSUM} ${pfx2asdir}/${pfx2asfile} > ${mondatadir}/logs/${MON}.ext.${ts}.log 2>&1
error_exit $? "$MON:$ts: extracting raw targets failed. Exiting."
ln -s -f ${mondatadir}/${MON}.link2dest.${ts}.raw ${mondatadir}/${MON}.link2dest.raw

echo "$MON:$ts: selecting targets"
python ${codedir}/select_targets_lnk.py -l -o ${mondatadir}/${MON}.link2dest.max3.${ts}.out ${mondatadir}/${MON}.link2dest.raw ${mondatadir}/${MON}.link2dest.max3.out.prev $CSUM > ${mondatadir}/logs/${MON}.sel_lnk.${ts}.log 2>&1
error_exit $? "$MON:$ts: selecting targets failed."
ln -s -f ${mondatadir}/${MON}.link2dest.max3.${ts}.out ${mondatadir}/${MON}.link2dest.max3.out
    
echo "$MON:$ts: updating monstate DB with link2dest meta info"
python ${codedir}/update_db_monstate_meta.py -l ${memfsdir}/${MON}.monstate.lnk.db ${mondatadir}/${MON}.link2dest.max3.out $ts > ${mondatadir}/logs/${MON}.meta_lnk.${ts}.log 2>&1
error_exit $? "$MON:$ts: update DB link2dest meta failed."
awk '{print $1,"foo",$3,$4,0,$6; print $2,"foo",$3,$4+1,0,$6}' ${mondatadir}/${MON}.link2dest.max3.out > ${mondatadir}/${MON}.dot1.max3.${ts}.targets

ln -s -f ${mondatadir}/${MON}.dot1.max3.${ts}.targets ${mondatadir}/${MON}.dot1.max3.targets

echo "$MON:$ts: udpating monstatedb tsmap"
python ${codedir}/update_db_monstate_ts.py ${memfsdir}/${MON}.monstate.lnk.db ${ts} ${wallts}

echo "$MON:$ts: logging last update timestamp"
echo $ts > ${mondatadir}/${MON}.last_update_ts.txt

echo "$MON:$ts: copying monstate DB back to original location"
rsync --remove-source-files ${memfsdir}/${MON}.monstate.lnk.db ${mondatadir}/${MON}.monstate.lnk.db

echo "removing lock file ${mondatadir}/$MON.db-lock";
rm -f ${mondatadir}/${MON}.db-lock;

echo "$MON:$ts: pushing target list to monitor"
#<use whatever mechanism you use to push the target list to the monitor that does the probing>

echo "$MON:$ts: converting target list to sc_attach format"
python ${codedir}/convert-targets-scattach.py $MON ${mondatadir}/${MON}.dot1.max3.targets ${mondatadir}/${MON}.scattach.${ts}.targets
ln -s -f ${mondatadir}/${MON}.scattach.${ts}.targets ${mondatadir}/${MON}.scattach.targets

if [ $ONEOFF -eq "0" ]; then
    echo "copying linkhist file to history"
    mv ${mondatadir}/${MON}.lnkhist.cnt.${ts}.txt ${mondatadir}/history/
fi

echo "$MON:$ts: checking DB for errors.."
python ${codedir}/check_db_monstate_lnk.py -b ${mondatadir}/${MON}.monstate.lnk.db ${ts};

compress_warts
compress_backups
compress_logs

exit 0
