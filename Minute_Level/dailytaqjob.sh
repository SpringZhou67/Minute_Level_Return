#!/bin/bash
#$ -cwd
#$ -m abe
#$ -M spring.zhou@wustl.edu
#$ -o ge_logs/
#$ -e ge_errs/


echo "Starting Job at `date`: $DATEPREFIX $SUBSAMPLE"
sas ProgramMinuteLevelReturn.sas -log sas_logs/dailytaq.$DATEPREFIX.log 
echo "Ending Job at `date`: $DATEPREFIX $SUBSAMPLE"
