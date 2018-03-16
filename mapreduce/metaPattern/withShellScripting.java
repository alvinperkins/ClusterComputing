#!/bin/bash
JAR_FILE="mrdp.jar"
JOB_CHAIN_CLASS="mrdp.ch6.JobChainingDriver"
PARALLEL_JOB_CLASS="mrdp.ch6.ParallelJobs"
HADOOP="$( which hadoop )"
POST_INPUT="posts"
USER_INPUT="users"
JOBCHAIN_OUTDIR="jobchainout"
BELOW_AVG_INPUT="${JOBCHAIN_OUTDIR}/belowavg"
ABOVE_AVG_INPUT="${JOBCHAIN_OUTDIR}/aboveavg"
BELOW_AVG_REP_OUTPUT="belowavgrep"
ABOVE_AVG_REP_OUTPUT="aboveavgrep"
JOB_1_CMD="${HADOOP} jar ${JAR_FILE} ${JOB_CHAIN_CLASS} ${POST_INPUT} \
 ${USER_INPUT} ${JOBCHAIN_OUTDIR}"
JOB_2_CMD="${HADOOP} jar ${JAR_FILE} ${PARALLEL_JOB_CLASS} ${BELOW_AVG_INPUT} \
 ${ABOVE_AVG_INPUT} ${BELOW_AVG_REP_OUTPUT} ${ABOVE_AVG_REP_OUTPUT}"
CAT_BELOW_OUTPUT_CMD="${HADOOP} fs -cat ${BELOW_AVG_REP_OUTPUT}/part-*"
CAT_ABOVE_OUTPUT_CMD="${HADOOP} fs -cat ${ABOVE_AVG_REP_OUTPUT}/part-*"
RMR_CMD="${HADOOP} fs -rmr ${JOBCHAIN_OUTDIR} ${BELOW_AVG_REP_OUTPUT} \
 ${ABOVE_AVG_REP_OUTPUT}"
LOG_FILE="avgrep_`date +%s`.txt"


{
 echo ${JOB_1_CMD}
 ${JOB_1_CMD}
 if [ $? -ne 0 ]
 then
 echo "First job failed!"
 echo ${RMR_CMD}
 ${RMR_CMD}
 exit $?
 fi
 echo ${JOB_2_CMD}
 ${JOB_2_CMD}
 if [ $? -ne 0 ]
 then
 echo "Second job failed!"
 echo ${RMR_CMD}
 ${RMR_CMD}
 exit $?
 fi
 echo ${CAT_BELOW_OUTPUT_CMD}
 ${CAT_BELOW_OUTPUT_CMD}
 echo ${CAT_ABOVE_OUTPUT_CMD}
 ${CAT_ABOVE_OUTPUT_CMD}
 echo ${RMR_CMD}
 ${RMR_CMD}
 exit 0
} &> ${LOG_FILE}


