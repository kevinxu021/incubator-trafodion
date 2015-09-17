USAGE="usage: load.debitcredit.sh
    [ -db|--database|database <<database>> ]
    [ -sch|--schema|schema <<database>> ]
    [ -sc|--scale|scale <<scalefactor>> ]
    [ -tb|--table|table <<tablename>> ]
    [ -s|--streams|streams <<streams>> ] 
    [ -p|--partitions|partitions <<partitions>> ] 
    [ -c|--compress|compress ]
    [ -d|--debug|debug ]
    [ -id|--testid|testid <<testid>> ]
    [ -o|--options|options <<options>> ]
    [ -h|--help|help ]"
    
#Default Values
BENCHMARK=DebitCredit
DATABASE=${JAVABENCH_DEFAULT_DATABASE}
SCHEMA=default
TABLE=default
TESTID=$(date +%y%m%d.%H%M)
SCALE=$DEBITCREDIT_SCALE
if (( $MAX_MXOSRVR < 32 )) ; then STREAMS=$MAX_MXOSRVR; else STREAMS=32; fi
OPTION_COMPRESS=FALSE
PARTITIONS=$SYSTEM_DEFAULT_PARTITIONS

while [[ $# > 0 ]] ; do
key="$1"; shift;
case ${key,,} in
    -db|--database|database)		DATABASE="$1"; shift;;
    -sch|--schema|schema)		SCHEMA="$1"; shift;;
    -sc|--scale|scale)			SCALE="$1"; shift;;
    -tb|--table|table)                  TABLE="$1"; shift;;
    -s|--streams|streams)		STREAMS="$1"; shift;;
    -p|--partitions|partitions)		PARTITIONS="$1"; shift;; 
    -c|--compress|compress)		OPTION_COMPRESS="TRUE";;
    -id|--testid|testid)		export TESTID="$1"; shift;;
    -o|--options|options)		OPTIONS="$1"; shift;;
    -d|--debug|debug)			OPTION_DEBUG="TRUE";;
    -h|--help|help)			echo -e "$USAGE"; exit 1;;
    *)					echo "Invalid input switch: $key"; echo -e "$USAGE"; exit 1;;
esac
done

case ${DATABASE,,} in
	trafodion|traf)		DATABASE="Trafodion";;
	seaquest)		DATABASE="SeaQuest";;
	*)		
		echo "Invalid Database Specified. DATABASE = ${DATABASE}. Expected values: [ Trafodion | Hbase | SeaQuest ]"; 
		echo -e "$USAGE"; exit 1;;

esac

if [ -z "$LOGDIRECTORY" ]; then
export LOGDIRECTORY="${JAVABENCH_TEST_HOME}/log/${TESTID}.load.${BENCHMARK}.${DATABASE}/"
if [ ! -f "${LOGDIRECTORY}" ] ; then
mkdir -p ${LOGDIRECTORY}
fi
fi
LOGFILE=${LOGDIRECTORY}/${TESTID}.load.${BENCHMARK}.${DATABASE}.log

{
echo "
$0

           DATABASE = ${DATABASE}
             SCHEMA = ${SCHEMA}
              SCALE = ${SCALE}
              TABLE = ${TABLE}
               SALT = ${SALT}
            STREAMS = ${STREAMS}
             TESTID = ${TESTID}
        COMPRESSION = ${OPTION_COMPRESS}
            OPTIONS = ${OPTIONS}
       OPTION_DEBUG = ${OPTION_DEBUG}
                ( logs will be found in ${LOGDIRECTORY} )
"

if [[ ${TABLE} = "default" ]] ; then
TABLES="branch teller account history"
else
TABLES=$TABLE
fi

if [[ ${OPTION_COMPRESS} = "TRUE" ]] ; then
OPTIONS="compression ${OPTIONS}"
fi

echo "
===== Start ${BENCHMARK} Load ( TESTID = ${TESTID} ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

STARTTIME=$SECONDS

case ${DATABASE,,} in
	trafodion)	

		if [[ ${SCHEMA} = "default" ]] ; then
			SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/${DATABASE,,}.properties | awk '{ print $3 }')
		fi

for TABLENAME in ${TABLES} ; do

case ${TABLENAME,,} in

	branch)		
		# create 
java -Ddbconnect.properties=${DATABASE,,}.properties DebitCreditLoader $SCALE schema ${SCHEMA} table ${TABLENAME,,} createschema dropcreate ${OPTIONS}
		# partition		
pdsh -w ${SYSTEM_FIRST_NODE} "cd ${JAVABENCH_TEST_HOME};. profile;java GetDcSplitTableCommand table ${SCHEMA^^}.DC_${TABLENAME^^}_${SCALE} scalefactor ${SCALE} rowsperscale 1 partitions ${PARTITIONS} serialize | hbase shell"
		# align Tables
align.table.sh table ${SCHEMA^^}.DC_${TABLENAME^^}_${SCALE} testid ${TESTID}
		# load
java -Ddbconnect.properties=${DATABASE,,}.properties DebitCreditLoader $SCALE schema ${SCHEMA} table ${TABLENAME,,} load upsert usingload maintain ${OPTIONS}
		;;

	teller)		
		# create 
java -Ddbconnect.properties=${DATABASE,,}.properties DebitCreditLoader $SCALE schema ${SCHEMA} table ${TABLENAME,,} dropcreate ${OPTIONS}
		# partition		
pdsh -w ${SYSTEM_FIRST_NODE} "cd ${JAVABENCH_TEST_HOME};. profile;java GetDcSplitTableCommand table ${SCHEMA^^}.DC_${TABLENAME^^}_${SCALE} scalefactor ${SCALE} rowsperscale 10 partitions ${PARTITIONS} serialize | hbase shell"
		# align Tables
align.table.sh table ${SCHEMA^^}.DC_${TABLENAME^^}_${SCALE} testid ${TESTID}
		# load
java -Ddbconnect.properties=${DATABASE,,}.properties DebitCreditLoader $SCALE schema ${SCHEMA} table ${TABLENAME,,} load upsert usingload maintain ${OPTIONS}
		;;

	account)		
		# create 
java -Ddbconnect.properties=${DATABASE,,}.properties DebitCreditLoader $SCALE schema ${SCHEMA} table ${TABLENAME,,} dropcreate ${OPTIONS}
		# partition		
pdsh -w ${SYSTEM_FIRST_NODE} "cd ${JAVABENCH_TEST_HOME};. profile;java GetDcSplitTableCommand table ${SCHEMA^^}.DC_${TABLENAME^^}_${SCALE} scalefactor ${SCALE} rowsperscale 100000 partitions ${PARTITIONS} serialize | hbase shell"
		# align Tables
align.table.sh table ${SCHEMA^^}.DC_${TABLENAME^^}_${SCALE} testid ${TESTID}
		# load
java -Ddbconnect.properties=${DATABASE,,}.properties DebitCreditLoader $SCALE schema ${SCHEMA} table ${TABLENAME,,} load upsert usingload batchsize 1000 streams $STREAMS maintain ${OPTIONS}
		;;

	history)		
		# create 
java -Ddbconnect.properties=${DATABASE,,}.properties DebitCreditLoader $SCALE schema ${SCHEMA} table ${TABLENAME,,} dropcreate maintain ${OPTIONS}
		# partition		
pdsh -w ${SYSTEM_FIRST_NODE} "cd ${JAVABENCH_TEST_HOME};. profile;java GetDcSplitTableCommand table ${SCHEMA^^}.DC_${TABLENAME^^}_${SCALE} scalefactor ${SCALE} rowsperscale 1 partitions ${PARTITIONS} serialize | hbase shell"
		# align Tables
align.table.sh table ${SCHEMA^^}.DC_${TABLENAME^^}_${SCALE} testid ${TESTID}
		;;

	*)		
		echo "Invalid Table Specified. TABLENAME = ${TABLENAME}. Expected values: [ branch | teller | account | history ]"; 
		echo -e "$USAGE"; exit 1;;
esac

done

		;;

	seaquest)

		if [[ ${SCHEMA} = "default" ]] ; then
		SCHEMA=$(grep schema ${JAVABENCH_TEST_HOME}/${DATABASE,,}.properties | awk '{ print $3 }')
		fi
		
		# create & load
		
for TABLENAME in ${TABLES} ; do

case ${TABLENAME,,} in

	branch)		
		# create & load
java -Ddbconnect.properties=${DATABASE,,}.properties DebitCreditLoader $SCALE schema ${SCHEMA} table ${TABLENAME,,} createschema dropcreate load maintain ${OPTIONS}
		;;

	teller)		
		# create & load
java -Ddbconnect.properties=${DATABASE,,}.properties DebitCreditLoader $SCALE schema ${SCHEMA} table ${TABLENAME,,} dropcreate load maintain ${OPTIONS}
		;;

	account)		
		# create & load
java -Ddbconnect.properties=${DATABASE,,}.properties DebitCreditLoader $SCALE schema ${SCHEMA} table ${TABLENAME,,} dropcreate load batchsize 1000 streams $STREAMS maintain ${OPTIONS}
		;;

	history)		
		# create
java -Ddbconnect.properties=${DATABASE,,}.properties DebitCreditLoader $SCALE schema ${SCHEMA} table ${TABLENAME,,} dropcreate maintain ${OPTIONS}
		;;

	*)		
		echo "Invalid Table Specified. TABLENAME = ${TABLENAME}. Expected values: [ branch | teller | account | history ]"; 
		echo -e "$USAGE"; exit 1;;
esac

done

		;;
esac
  
echo "
===== End ${BENCHMARK} Load ( Elapsed Seconds = $(( $SECONDS - $STARTTIME )) ) =====  [ $(date +"%Y-%m-%d %H:%M:%S") ]
"

} | tee ${LOGFILE}

