#!/bin/bash

admin=""
mybin=""
db=""
dbuser=""
export PGPASSWORD=''
host=""
subject="Bacula backup report for `date +%D`"


#Check if there's any job failure
header="\n\n<b>Failed backup task</b>\n<hr>\n"
footer="\n<hr><br>\n\n"
check_fail_record=`echo "select count(*) FROM Job WHERE  jobstatus!='T' AND Type='B' AND RealEndTime >= now() - INTERVAL '168 HOUR'" | ${mybin} -h ${host} -t -U ${dbuser} ${db} | xargs`
if [ ${check_fail_record} != "0" ];
then
        msg=`echo "SELECT JobId, Name, StartTime, EndTime, Level, JobStatus, JobFiles, JobBytes FROM Job WHERE jobstatus!='T' AND Type='B' AND RealEndTime >= now() - INTERVAL '168 HOUR' ORDER BY JobId;" \
        | ${mybin} -h ${host} -t -U ${dbuser} ${db} | sed '/^$/d' \
        |awk -F "|" 'BEGIN {print "JobId\tName\tStart Time\tStop Time\tLevel\tStatus\tFiles\tBytes"} { printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%9.2f GB\n", $1, $2, $3, $4, $5, $6, $7, $8/(1024*1024*1024)); \
        sum+=$8} END {print "Total: "sum/(1024*1024*1024) " GB"}'| column -t -s $'\t'`
else
        msg="There's no failed job this week. Yaw! :)"
fi
output="${header}<pre>${msg}</pre>${footer}"


#check list of tapes ready for offsite transport
header="\n\n<b>Full tapes ready for offsite transport</b>\n<hr>\n"
footer="\n<hr><br>\n\n"
check_full_tape=`echo "select count(*) FROM media WHERE (poolid='3' or poolid='6') and volstatus='Full'" | ${mybin} -h ${host} -t -U ${dbuser} ${db} | xargs`
if [ ${check_full_tape} != "0" ];
then
        msg=`echo "SELECT volumename,slot,volstatus FROM media WHERE (poolid='3' or poolid='6') and volstatus='Full'" | ${mybin} -h ${host} -t -U ${dbuser} ${db} \
        | sed '/^$/d'| awk -F "|" 'BEGIN {print "Volume\tSlot\tStatus"} { printf("%s\t%s\t%s\n", $1, $2, $3) }' | column -t -s $'\t'`
else
        msg="There's no tape ready for pull out."
fi
output="${output}${header}<pre>${msg}</pre>${footer}"

#check list of expired tapes in offsite
header="\n\n<b>Expired tapes in offsite</b>\n<hr>\n"
footer="\n<hr><br>\n\n"
check_expired_tape=`echo "select count(*) FROM media WHERE (poolid='8' or poolid='9') \
and CAST(coalesce(extract('epoch' from lastwritten),'0') as integer)+volretention < extract('epoch' from now())" | ${mybin} -h ${host} -t -U ${dbuser} ${db} | xargs`
if [ ${check_expired_tape} != "0" ];
then
        msg=`echo "select volumename,volstatus,to_timestamp(CAST(coalesce(extract('epoch' from lastwritten),'0') as integer)+volretention) as expire FROM media \
        WHERE (poolid='8' or poolid='9') and CAST(coalesce(extract('epoch' from lastwritten),'0') as integer)+volretention < extract('epoch' from now())" \
        | ${mybin} -h ${host} -t -U ${dbuser} ${db} | sed '/^$/d'| awk -F "|" 'BEGIN {print "Name\tStatus\tExpire"} { printf("%s\t%s\t%s\n", $1, $2, $3) }' | column -t -s $'\t'`
else
        msg="There's no expired tape in offsite"
fi
output="${output}${header}<pre>${msg}</pre>${footer}"

#send email out
echo -e "${output}" | mail -s "$(echo -e "${subject}\nContent-Type: text/html")" ${admin} 
