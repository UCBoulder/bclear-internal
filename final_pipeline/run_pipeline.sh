#!/bin/bash 

start=`date +%s`
echo "******************************************************" >> get_transform_load_new.log
date >> get_transform_load_new.log
tar -xf $1.tar.gz -C ../.
rm $1.tar.gz

# operator unit_id identification
#echo "**************STAGE 1: Unit ID Classification to Operator and Operator Technology**************" >> get_transform_load_new.log
#python3 operator_identification.py --data-folder $1
#RESULT=$?
#if [ $RESULT -ne 0 ];then 
#   echo "Operator identification failed. exitiing the pipeline."
#   exit 1
#fi
#echo "**************STAGE 1: Completed**************" >> get_transform_load_new.log

# validating the data
echo "**************STAGE 2: Data Validation**************" >> get_transform_load_new.log
python3 filter_data.py --data-folder $1
RESULT=$?
if [ $RESULT -ne 0 ];then 
    echo "Data validation failed. exiting the pipeline."
    exit 1
fi
echo "**************STAGE 2: Completed**************" >> get_transform_load_new.log

# ingest to elasticsearch 
echo "**************STAGE 3: Data Upload**************" >> get_transform_load_new.log
python3 bulk_upload.py --data-folder $1
RESULT=$?
if [ $RESULT -ne 0 ];then 
    echo "Data upload failed. exiting the pipeline."
    exit 1
fi
echo "**************STAGE 3: Completed**************" >> get_transform_load_new.log
rm -rf $1/validated_data

tar -zcf $1.tar.gz $1 2>&1 >> get_transform_load_new.log
rm -rf $1 2>&1 >> get_transform_load_new.log

date >> get_transform_load_new.log
end=`date +%s`
runtime=$((end-start))
echo "The script took a total time of ${runtime}" >> get_transform_load_new.log
echo "******************************************************" >> get_transform_load_new.log
