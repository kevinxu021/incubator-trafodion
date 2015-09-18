USAGE="usage: compact.table.sh
    Flushes the memcache for all tables involved with a benchmark.
    Options:
      [ -bm|--benchmark|benchmark <<benchmark>> ]
      [ -db|--database|database <<database>> ]
      [ -sch|--schema|schema <<schema>> ]
      [ -sc|--scale|scale <<scalefactor>> ]
      [ -h|--help|help ]
"

BENCHMARK=default
DATABASE=trafodion
SCHEMA=default
SCALE=default

while [[ $# > 0 ]] ; do
key="$1"; shift;
case $key in
    -bm|--benchmark|benchmark)		BENCHMARK="$1"; shift;;
    -db|--database|database)		DATABASE="$1"; shift;;
    -sch|--schema|schema)		SCHEMA="$1"; shift;;
    -sc|--scale|scale)			SCALE="$1"; shift;;
    -h|--help|help)			echo -e "$USAGE"; exit 1;;
    *)					echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

if [[ ${SCHEMA} = "default" ]] ; then
SCHEMA=javabench
fi

if [[ ${BENCHMARK} = "ycsb" ]] ; then

BENCHMARK_PREFIX="YCSB_TABLE"
if [[ ${SCALE} = "default" ]] ; then
SCALE_SUFFIX="20"
else
SCALE_SUFFIX="${SCALE}"
fi

elif [[ ${BENCHMARK} = "debitcredit" ]] ; then

BENCHMARK_PREFIX="DC"
if [[ ${SCALE} = "default" ]] ; then
SCALE_SUFFIX="1024"
else
SCALE_SUFFIX="${SCALE}"
fi

elif [[ ${BENCHMARK} = "orderentry" ]] ; then

BENCHMARK_PREFIX="OE"
if [[ ${SCALE} = "default" ]] ; then
SCALE_SUFFIX="1024"
else
SCALE_SUFFIX="${SCALE}"
fi

elif [[ ${BENCHMARK} = "ed" ]] ; then

BENCHMARK_PREFIX="ED_TABLE"
SCALE_SUFFIX=""

else

echo "Invalid benchmark specified.  Expected ycsb, debitcredit, orderentry or ed"
echo -e "$USAGE"
exit 1

fi


echo "Preparing to compact ${SCHEMA^^}.${BENCHMARK_PREFIX^^}*${SCALE_SUFFIX^^}"

tableList=$(echo "list '${SCHEMA^^}.${BENCHMARK_PREFIX^^}*${SCALE_SUFFIX^^}'" | hbase shell 2>/dev/null | \
    awk 'BEGIN {want=0}
         /^TABLE/ {want=1; next}
         /row\(s\)/ {exit}
                  {if (want==1) {print $1}}
        ')

if [ ${#tableList} -eq 0 ] ; then
    echo "No tables were found."
    exit 0
fi

compTableList=$(echo "${tableList}" | sed -e 's/^/compact \x27/g' -e 's/$/\x27/g')
echo "${compTableList}" | hbase shell

echo ""
echo "Now verifying that all tables have been compacted."
echo ""

for currTable in ${tableList}
do
    while true ; do
        currStat=$(curl -sS --noproxy '*' http://${SYSTEM_NAMEMGR_URL}:${SYSTEM_NAMEMGR_PORT}/table.jsp?name=${currTable} 2>/dev/null | head -100 | \
            awk 'BEGIN {want=0; lastStat=""}
                 /Table Attributes/ {want=1;next}
                 /Compaction/ {if (want==1) {want=2; next}}
                 /\/td/ {if (want==2) {print lastStat; exit}}
                 {if (want==2) {if (index($0,"NONE") > 0) {print "NONE"; exit} else {if (length($1)>0) {lastStat=$1}}}}
                ')
        echo "Table: ${currTable} - compaction: ${currStat}"
        if [ X${currStat} == XNONE ] ; then
            break
        fi
        sleep 5
    done
done
     
exit 0