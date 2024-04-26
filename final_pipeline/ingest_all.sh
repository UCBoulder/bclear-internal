#!/bin/bash

tar -xf ../2014.tar.gz -C ../.
./run_pipeline.sh '../201401'
./run_pipeline.sh '../201402'
./run_pipeline.sh '../201403'
./run_pipeline.sh '../201404'
./run_pipeline.sh '../201405'
./run_pipeline.sh '../201406'
./run_pipeline.sh '../201407'
./run_pipeline.sh '../201408'
./run_pipeline.sh '../201409'
./run_pipeline.sh '../201410'
./run_pipeline.sh '../201411'
./run_pipeline.sh '../201412'
rm ../2014.tar.gz
tar --remove-files  -zcf ../2014.tar.gz ../2014*

tar -xf ../2013.tar.gz -C ../.
./run_pipeline.sh '../201301'
./run_pipeline.sh '../201302'
./run_pipeline.sh '../201303'
./run_pipeline.sh '../201304'
./run_pipeline.sh '../201305'
./run_pipeline.sh '../201306'
./run_pipeline.sh '../201307'
./run_pipeline.sh '../201308'
./run_pipeline.sh '../201309'
./run_pipeline.sh '../201310'
./run_pipeline.sh '../201311'
./run_pipeline.sh '../201312'
rm ../2013.tar.gz
tar --remove-files  -zcf ../2013.tar.gz ../2013*

tar -xf ../2012.tar.gz -C ../.
./run_pipeline.sh '../201201'
./run_pipeline.sh '../201202'
./run_pipeline.sh '../201203'
./run_pipeline.sh '../201204'
./run_pipeline.sh '../201205'
./run_pipeline.sh '../201206'
./run_pipeline.sh '../201207'
./run_pipeline.sh '../201208'
./run_pipeline.sh '../201209'
./run_pipeline.sh '../201210'
./run_pipeline.sh '../201211'
./run_pipeline.sh '../201212'
rm ../2012.tar.gz
tar --remove-files  -zcf ../2012.tar.gz ../2012*
