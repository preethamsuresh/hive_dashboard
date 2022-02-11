#!/bin/bash
#set -x
export JAVA_HOME=/usr/java/default
#export HADOOP_OPTS="-Xms16000m -Xmx20000m $HADOOP_OPTS"
cd /lowes/hive_metrics/NEW
today=$(date "+%Y_%m_%d")
export HADOOP_CLIENT_OPTS="-Djline.terminal=jline.UnsupportedTerminal"
inputFolder=$today/"inputFiles"
mkdir -p $inputFolder
#cd $today
 
MysqlUser="root"
MysqlPwd="bigdata9"
HiveMetaServer="lxhdpmastqa004"
hiveDump=$today/"inputFiles"/"hive_dump_"$today".sql"
hiveTableMeta=$today/"inputFiles"/"hive_table_meta_"$today
hiveTableLocationMeta=$today/"inputFiles"/"hive_table_location_meta_"$today
emailId=prsuresh@lowes.com
#emailId=DL-GO-HADOOP-ADMIN@lowes.com
fsimageFile=$inputFolder/"fsimage_"$today
#lastAccessData=$today/"splits_output_combined_"$today".sql"
todaySplits=$today"_hive_table_meta_splits"
finalOutput=$today"_splits_all_output"
hiveTableLoactionMapping=$today/"HIVE_META_HDFS_LOCATION_MAPPING_dump_"$today
 

runBeelineQuery(){
    echo "running  query : $1"
    if
        /usr/local/bin/beeline -u 'jdbc:hive2://lxhdpmastqa003:10001/hive_metrics;transportMode=http;httpPath=cliservice?tez.queue.name=default' -n prsuresh --showHeader=false --outputformat=csv2 -e "$1"
    then
        echo "query completed"
    else
        exiting "query run failed : $1"
    fi
    echo "Query complete"
}
exiting(){
    echo "ERROR occured at $1 "
    echo "exiting !"
    echo "ERROR occured at $1 .Please check."| mail -s "hive_metrics alert" $emailId
    exit 1
}
copyFsimageFile(){
    echo "FsimageFile copy process ... "
    file=$(ssh lxhdpmastqa002 "ls /lowes/hadoop/hdfs/namenode/current/fsimage_*|grep -v md5|head -n1|awk '{print $9}'")
#/lowes/hadoop/hdfs/namenode/current/fsimage_*|grep -v md5|head -n1|awk '{print $9}'
    fileName=$(echo $file|awk -F '/' '{print $7}')
    echo "Fsimage file  : $fileName"
    echo "scp of fsimage file start"
    if
        scp lxhdpmastqa002:$file $fsimageFile
    then
        echo "scp of $fsimage_file successfull"
    else
        exiting "fsimage scp failure"
    echo "FsimageFile copy Process complete."
    fi
}
copyHiveDump(){
    echo "Hive Dump copy process started..."
    if
        ssh $HiveMetaServer  "mysqldump -u$MysqlUser -p$MysqlPwd  hive" > $hiveDump
    then
        echo "hiveDump successfully copied to : $hiveDump "
    else
        exiting "hiveDump retreival  failure"
 
    echo "Hive Dump copy process complete."
    fi
}
copyHiveMeta(){
    echo "Hive meta queries run begin"
    if
        ssh $HiveMetaServer "mysql -u$MysqlUser -p$MysqlPwd -s -N -e \"use hive;select concat(T.TBL_ID,'~',D.NAME,'~',T.CREATE_TIME,'~',T.TBL_NAME),D.NAME,T.TBL_NAME FROM TBLS T JOIN DBS D ON T.DB_ID=D.DB_ID ;\""> $hiveTableMeta
    then
        echo "hive tables meta query run complete and stored at $hiveTableMeta"
    else
        exiting "hive tables meta query run failure"
    fi
    if
        ssh $HiveMetaServer "mysql -u$MysqlUser -p$MysqlPwd -s -N -e \"use hive; SELECT TBL_ID,SD_ID,PARTITION_TYPE,CONCAT(SUBSTR(LOCATION,19),'/') FROM (SELECT P.TBL_ID,P.SD_ID,1 AS PARTITION_TYPE ,S.LOCATION FROM PARTITIONS P JOIN SDS S ON P.SD_ID=S.SD_ID UNION ALL SELECT T.TBL_ID,T.SD_ID,0 AS PARTITION_TYPE,S.LOCATION FROM TBLS T JOIN SDS S ON T.SD_ID=S.SD_ID WHERE T.TBL_ID NOT IN (SELECT TBL_ID FROM PARTITIONS) AND T.TBL_TYPE IN ('EXTERNAL_TABLE','MANAGED_TABLE') )A\" " > $hiveTableLocationMeta
    then
        echo "hive tables location  query run complete and stored at $hiveTableLocationMeta"
    else
        exiting "hive tables location  query run failure"
    fi
    echo "Hive meta queries run complete"
}
getInputFiles(){
    echo "getting all input files ..."
    copyFsimageFile
    copyHiveDump
    copyHiveMeta
    echo "all input files retrieved"
}
 
tablesLastAccess(){
    splitsDir=$today/$todaySplits
    mkdir -p $splitsDir
    split --lines 2000 $hiveTableMeta $splitsDir/$today"_split_"
    IFS=$'\n'
    echo "Finding tables last access process started.."
    echo "Preparing sqls from split files.."
    for j in $(ls $splitsDir)
    do
        echo "Preparing sql for split file: $j"
        for i in $(cat $splitsDir/$j)
        do
            data=$(echo $i|awk '{print $1}')
            db=$(echo $i|awk '{print $2}')
            table=$(echo $i|awk '{print $3}')
            echo  "select 'new_record:$data';" >>$splitsDir/$j"_query.sql"
            echo -n "use $db;" >>$splitsDir/$j"_query.sql"
            echo "show table extended like $table;" >>$splitsDir/$j"_query.sql"
            echo "select 'record_end:record_end';" >>$splitsDir/$j"_query.sql"
       # /usr/local/bin/beeline -u 'jdbc:hive2://lxhdpmastqa004:10001/batch;transportMode=http;httpPath=cliservice?tez.queue.name=batch' -n hdpbatch --showHeader=false --outputformat=csv2 -f $today_splits/$j"_query.sql" |egrep "new_record:|tableName:|lastAccessTime:|lastUpdateTime:|record_end:"|awk -F':' '{ORS = "~"}{print $2}'|sed 's/~record_end~/\n/g' >> $today_splits/$j"_res.txt"
        done
        echo "query preparation completed: "$j"_query.sql"
    done
    echo "sqls for all split files prepared."
    echo "executing split file sqls"
 
    for i in $(ls $splitsDir/*sql)
    do
        if
            /usr/local/bin/beeline --force=true  -u 'jdbc:hive2://lxhdpmastqa003:10001/batch;transportMode=http;httpPath=cliservice?tez.queue.name=batch' -n prsuresh --showHeader=false --outputformat=csv2 -f $i  |egrep "new_record:|lastAccessTime:|lastUpdateTime:|record_end:"|awk -F':' '{ORS = "~"}{print $2}'|sed 's/~record_end~/\n/g' > $i".out" &
        then
            echo "split file sql $i has executed successfully  : "$i".out"
        else
            exiting "split file sql $i execution failed."
        fi
    done
    wait
    echo "executing split file sqls completed ."
 
    echo "cleaning split file output files"
    for i in $(cat $splitsDir/*out)
    do
        echo "cleaning split file output : $i"
        no_of_columns=$(awk -F'~' '{print 6-NF}'<<< $i)
        for j in $(seq 1 $no_of_columns)
        do
            i+="~0"                        #replacing missing cloumn values with "0"
            echo $i
        done
        echo $i >> $today/$finalOutput
    done
    sed -i 's/unknown/0/g' $today/$finalOutput   #replacing column value "unknown" with "0"
    echo "Finding tables last access process completed.."
    echo "tables last access data : $today/$finalOutput"
 
}
 
decodeFsimage(){
#    export HADOOP_OPTS="-Xms16000m -Xmx20000m $HADOOP_OPTS"
    mkdir -p $today/temp
    echo "decoding fsimage"
    if
        hdfs oiv -p Delimited -delimiter "#" -i $fsimageFile -o $today/"fsimage_delimited_"$today -t $today/temp/fsimage_oiv_tmp &> $today/"fsimage_delimited_"$today.log
    then
        echo "fsimage oiv decode successfull !!"
        rm -r $today/temp
    else
        echo "fsimage oiv decode failure; exiting"
        rm -r $today/temp
        exiting "fsimage decode"
    fi
 
    echo "splitting the fsimage delimited file.."
    if
        mkdir $today/fsimage_delimited_spilts
        split --verbose -l 1000000 $today/"fsimage_delimited_"$today $today/fsimage_delimited_spilts/fsimage_split_
    then
        echo "fsimage splitted into 1050000 row files"
    else
        exiting "fsimage splitting error"
    fi
    #echo "deleting old fsimage_split files , if existed ."
    #rm -r $today/fsimage_delimited_spilts/fsimage_split_*
    #echo "deleting old fsimage_split files , if existed . complete"
    echo "deleting old fsimages files in hdfs"
    hdfs dfs -rm hdfs://HDPSAPRODHA/hdptmp/fsimage/*
    echo "deleted old fsimages files in hdfs"
    if
        hdfs dfs -put $today/fsimage_delimited_spilts/* hdfs://HDPSAPRODHA/hdptmp/fsimage/
    then
        echo "fsimage load to hdfs successfull"
    else
        exiting "fsimage load to hdfs"
    fi
    echo "deleting the fsimage splits"
    if rm -r $today/fsimage_delimited_spilts/fsimage_split_* ;then echo "fsimage splits are deleted";else exiting "fsimage splits deletion" ;fi
    echo "fsimage decode process complete."
}
 
tableLocationMapping(){
    echo "create HIVE_TABLE_LOCATION_META table backup.."
    runBeelineQuery "drop table if exists HIVE_TABLE_LOCATION_META_bkp;create table HIVE_TABLE_LOCATION_META_bkp as select * from HIVE_TABLE_LOCATION_META"
    echo "create HIVE_TABLE_LOCATION_META table"
    runBeelineQuery "drop table if exists HIVE_TABLE_LOCATION_META;create table HIVE_TABLE_LOCATION_META(TBL_ID int,SD_ID int,PARTITION_TYPE int,LOCATION varchar(1000)) row format delimited fields terminated by '\t' stored as textfile;"
    if
        [[ $(hdfs dfs  -ls hdfs://HDPSAPRODHA/hdptmp/hive_table_location_meta/|wc -l) != 0 ]]
    then
        exiting "hive_table_location_meta file still exists in hdfs, remove it"
    fi
    if
        hdfs dfs -put $hiveTableLocationMeta hdfs://HDPSAPRODHA/hdptmp/hive_table_location_meta
    then
        echo "hive_table_location_meta file loaded to hdfs successfully"
    else
        exiting "hive_table_location_meta file load to hdfs"
    fi
    echo "load HIVE_TABLE_LOCATION_META table"
    runBeelineQuery "load data  inpath '/hdptmp/hive_table_location_meta' into table HIVE_TABLE_LOCATION_META"
    echo "create HIVE_TABLE_LOCATION_HDFS_SIZE table backup"
    runBeelineQuery "drop table if exists HIVE_TABLE_LOCATION_HDFS_SIZE_bkp;create table HIVE_TABLE_LOCATION_HDFS_SIZE_bkp as select * from HIVE_TABLE_LOCATION_HDFS_SIZE"
    echo "create HIVE_TABLE_LOCATION_HDFS_SIZE table"
    runBeelineQuery "drop table if exists HIVE_TABLE_LOCATION_HDFS_SIZE;create table HIVE_TABLE_LOCATION_HDFS_SIZE AS select path,count(*) as NO_OF_FILES,SUM(fsize) as location_size,round(cast(SUM(fsize)/count(*) as bigint),2) as AVG_FILE_SIZE_BYTES, round(cast(SUM(fsize)/count(*) as bigint)/1024,2) as AVG_FILE_SIZE_KB,round(cast(SUM(fsize)/count(*) as bigint)/1024/1024,2) as AVG_FILE_SIZE_MB,round(cast(SUM(fsize)/count(*) as bigint)/1024/1024/1024,2) as AVG_FILE_SIZE_GB from file_info where path in (select location FROM HIVE_TABLE_LOCATION_META) group by path,depth;"
    echo "create HIVE_META_HDFS_LOCATION_MAPPING table backup"
    runBeelineQuery "drop table if exists HIVE_META_HDFS_LOCATION_MAPPING_bkp;create table HIVE_META_HDFS_LOCATION_MAPPING_bkp as select * from HIVE_META_HDFS_LOCATION_MAPPING"
    echo "create HIVE_META_HDFS_LOCATION_MAPPING table"
    #runBeelineQuery "drop table HIVE_META_HDFS_LOCATION_MAPPING;create table HIVE_META_HDFS_LOCATION_MAPPING as select M.tbl_id,M.sd_id,M.partition_type,M.location,CASE WHEN S.path is null then 'NO' else 'YES' end ,S.location_size,S.no_of_files,S.avg_file_size_bytes,S.avg_file_size_kb,S.avg_file_size_mb,S.avg_file_size_gb,0 as table_size from HIVE_TABLE_LOCATION_META M LEFT OUTER JOIN HIVE_TABLE_LOCATION_HDFS_SIZE S ON M.location=S.path"
    runBeelineQuery "drop table HIVE_META_HDFS_LOCATION_MAPPING;create table HIVE_META_HDFS_LOCATION_MAPPING as select M.tbl_id,M.sd_id,M.partition_type,M.location,CASE WHEN S.path is null then 'NO' else 'YES' end as location_exists,'' as all_table_locations_exists,S.location_size,S.no_of_files,S.avg_file_size_bytes,S.avg_file_size_kb,S.avg_file_size_mb,S.avg_file_size_gb,0 as table_size from hive_table_location_meta M LEFT OUTER JOIN hive_table_location_hdfs_size S ON M.location=S.path"
    echo "dump HIVE_META_HDFS_LOCATION_MAPPING table"
    #runBeelineQuery "select * from HIVE_META_HDFS_LOCATION_MAPPING " > hiveTableLoactionMapping
    if
        #/usr/local/bin/beeline -u 'jdbc:hive2://lxhdpmastqa003:10001/hive_metrics;transportMode=http;httpPath=cliservice?tez.queue.name=default' -n prsuresh --showHeader=false --outputformat=csv2 -e "select * from HIVE_META_HDFS_LOCATION_MAPPING " > $hiveTableLoactionMapping
        /usr/local/bin/beeline -u 'jdbc:hive2://lxhdpmastqa003:10001/hive_metrics;transportMode=http;httpPath=cliservice?tez.queue.name=default'  --showHeader=false --outputformat=dsv --delimiterForDSV=^ -nprsuresh  -e "select * from HIVE_META_HDFS_LOCATION_MAPPING " > $hiveTableLoactionMapping
 
    then
        echo "query completed"
    else
        exiting "query run failed : $1"
    fi
}
 
createFsimageTables(){
    echo "creating fsimage table backup"
#   runBeelineQuery "drop table if exists fsimage_bkp;create table fsimage_bkp as select * from fsimage"
    echo  "creating fsimage table "
    runBeelineQuery "drop table if exists fsimage;CREATE  TABLE fsimage( path varchar(2000),repl int,mdate date,atime date,pblksize int,blkcnt bigint,fsize bigint,nsquota bigint,dsquota bigint,permi varchar(100),usrname varchar(100),grpname varchar(100)) ROW FORMAT DELIMITED FIELDS TERMINATED BY '#';load data inpath '/hdptmp/fsimage/' into table fsimage ;"
    echo "backup file_info table"
    runBeelineQuery "drop table if exists file_info_bkp;create table file_info_bkp as select * from file_info "
    echo "create file_info table"
    runBeelineQuery "drop table file_info;CREATE TABLE file_info( path varchar(200),fsize bigint,usrname varchar(100),depth int) STORED AS ORC;"
    echo "load data into file_info table"
    runBeelineQuery "INSERT INTO TABLE file_info select concat('/' , split(path,'/')[1] , '/') as path, fsize ,usrname, 1 from fsimage;INSERT INTO TABLE file_info select concat('/' , split(path,'/')[1] , '/' , split(path,'/')[2] , '/') as path, fsize ,usrname, 2 from fsimage;INSERT INTO TABLE file_info select concat('/' , split(path,'/')[1] , '/' , split(path,'/')[2] , '/', split(path,'/')[3] , '/') as path, fsize ,usrname, 3 from fsimage;INSERT INTO TABLE file_info select concat('/' , split(path,'/')[1] , '/' , split(path,'/')[2] , '/', split(path,'/')[3] , '/', split(path,'/')[4] , '/') as path, fsize ,usrname, 4 from fsimage;INSERT INTO TABLE file_info select concat('/' , split(path,'/')[1] , '/' , split(path,'/')[2] , '/', split(path,'/')[3] , '/', split(path,'/')[4] , '/', split(path,'/')[5] , '/') as path, fsize ,usrname, 5 from fsimage;INSERT INTO TABLE file_info select concat('/' , split(path,'/')[1] , '/' , split(path,'/')[2] , '/', split(path,'/')[3] , '/', split(path,'/')[4] , '/', split(path,'/')[5] , '/' ,split(path,'/')[6] , '/') as path, fsize ,usrname, 6 from fsimage; INSERT INTO TABLE file_info select concat('/' , split(path,'/')[1] , '/' , split(path,'/')[2] , '/', split(path,'/')[3] , '/', split(path,'/')[4] , '/', split(path,'/')[5] , '/' ,split(path,'/')[6] , '/',split(path,'/')[7] , '/') as path, fsize ,usrname, 7 from fsimage;INSERT INTO TABLE file_info select concat('/' , split(path,'/')[1] , '/' , split(path,'/')[2] , '/', split(path,'/')[3] , '/', split(path,'/')[4] , '/', split(path,'/')[5] , '/' ,split(path,'/')[6] , '/',split(path,'/')[7] , '/',split(path,'/')[8] , '/') as path, fsize ,usrname, 8 from fsimage;INSERT INTO TABLE file_info select concat('/' , split(path,'/')[1] , '/' , split(path,'/')[2] , '/', split(path,'/')[3] , '/', split(path,'/')[4] , '/', split(path,'/')[5] , '/' ,split(path,'/')[6] , '/',split(path,'/')[7] , '/',split(path,'/')[8] , '/',split(path,'/')[9] , '/') as path, fsize ,usrname, 9 from fsimage;INSERT INTO TABLE file_info select concat('/' , split(path,'/')[1] , '/' , split(path,'/')[2] , '/', split(path,'/')[3] , '/', split(path,'/')[4] , '/', split(path,'/')[5] , '/' ,split(path,'/')[6] , '/',split(path,'/')[7] , '/',split(path,'/')[8] , '/',split(path,'/')[9] , '/',split(path,'/')[10] ,'/') as path, fsize ,usrname, 10 from fsimage;INSERT INTO TABLE file_info select concat('/' , split(path,'/')[1] , '/' , split(path,'/')[2] , '/', split(path,'/')[3] , '/', split(path,'/')[4] , '/', split(path,'/')[5] , '/' ,split(path,'/')[6] , '/',split(path,'/')[7] , '/',split(path,'/')[8] , '/',split(path,'/')[9] , '/',split(path,'/')[10] , '/',split(path,'/')[11] , '/') as path, fsize ,usrname, 11 from fsimage;INSERT INTO TABLE file_info select concat('/' , split(path,'/')[1] , '/' , split(path,'/')[2] , '/', split(path,'/')[3] , '/', split(path,'/')[4] , '/', split(path,'/')[5] , '/' ,split(path,'/')[6] , '/',split(path,'/')[7] , '/',split(path,'/')[8] , '/',split(path,'/')[9] , '/',split(path,'/')[10] , '/',split(path,'/')[11] , '/',split(path,'/')[12] , '/') as path, fsize ,usrname, 12 from fsimage;INSERT INTO TABLE file_info select concat('/' , split(path,'/')[1] , '/' , split(path,'/')[2] , '/', split(path,'/')[3] , '/', split(path,'/')[4] , '/', split(path,'/')[5] , '/' ,split(path,'/')[6] , '/',split(path,'/')[7] , '/',split(path,'/')[8] , '/',split(path,'/')[9] , '/',split(path,'/')[10] , '/',split(path,'/')[11] , '/',split(path,'/')[12] , '/',split(path,'/')[13] , '/') as path, fsize ,usrname, 13 from fsimage;INSERT INTO TABLE file_info select concat('/' , split(path,'/')[1] , '/' , split(path,'/')[2] , '/', split(path,'/')[3] , '/', split(path,'/')[4] , '/', split(path,'/')[5] , '/' ,split(path,'/')[6] , '/',split(path,'/')[7] , '/',split(path,'/')[8] , '/',split(path,'/')[9] , '/',split(path,'/')[10] , '/',split(path,'/')[11] , '/',split(path,'/')[12] , '/',split(path,'/')[13] , '/',split(path,'/')[14] , '/') as path, fsize ,usrname, 14 from fsimage;INSERT INTO TABLE file_info select concat('/' ,split(path,'/')[1] , '/',split(path,'/')[2] , '/',split(path,'/')[3] , '/',split(path,'/')[4] , '/',split(path,'/')[5] , '/',split(path,'/')[6] , '/',split(path,'/')[7] , '/',split(path,'/')[8] , '/',split(path,'/')[9] , '/',split(path,'/')[10] , '/',split(path,'/')[11] , '/',split(path,'/')[12] , '/',split(path,'/')[13] , '/',split(path,'/')[14] , '/',split(path,'/')[15] , '/') as path, fsize ,usrname, 15 from fsimage;"
    echo "load data into file_info complete"
}
runMysqlQuery(){
    echo "running mysql command $1"
    if
    mysql -u"$MysqlUser" -p"$MysqlPwd" -e"$1"
    then
        echo "Query run successful"
    else
        exiting "Mysql query : $1"
    fi
}
loadDashboardTables(){
    echo "create hive_metrics backup db "
    runMysqlQuery "drop database hive_metrics_bkp;create database hive_metrics_bkp"
    echo "backup  hive_metrics db in hive_metrics_bkp db"
    if
        mysqldump -u$MysqlUser -p$MysqlPwd hive_metrics | mysql -u$MysqlUser -p$MysqlPwd hive_metrics_bkp
    then
        echo "backedup  hive_metrics db in hive_metrics_bkp db successfully"
    else
        exiting "mysql hive_metrics db backup to  hive_metrics_bkp "
        #statements
    fi
    echo "create hive_metrics db"
    runMysqlQuery "drop database hive_metrics;CREATE DATABASE hive_metrics"
    echo "load hive_metrics db with new dump"
    if mysql -uroot -pbigdata9 hive_metrics < $hiveDump ;then echo "hive_metrics db loaded" ;else exiting "hive_metrics db load"; fi
    runMysqlQuery "use hive_metrics;create table if not exists HIVE_TABLES_LAST_ACCESS(TBL_ID BIGINT,DB_NAME VARCHAR(300),CREATE_TIME BIGINT,TBL_NAME VARCHAR(300),LAST_ACCESS_TIME
BIGINT,LAST_UPDATED_TIME BIGINT)"
    echo "mysql load last access table"
    runMysqlQuery "LOAD DATA LOCAL INFILE \"$today/$finalOutput\" INTO TABLE  hive_metrics.HIVE_TABLES_LAST_ACCESS FIELDS TERMINATED BY '~' LINES TERMINATED BY '\n'"
    echo "mysql create HIVE_META_HDFS_LOCATION_MAPPING table"
#    runMysqlQuery "use hive_metrics;create table HIVE_META_HDFS_LOCATION_MAPPING(tbl_id bigint,sd_id bigint,partition_type int,location varchar(1000),location_exists char(1),location_size bigint,no_of_files bigint,avg_file_size_bytes bigint,avg_file_size_kb double,avg_file_size_mb double,avg_file_size_gb double,table_size bigint)"
    runMysqlQuery "use hive_metrics;create table HIVE_META_HDFS_LOCATION_MAPPING(tbl_id bigint,sd_id bigint,partition_type int,location varchar(1000),location_exists char(3),all_table_locations_exists char(3),location_size  bigint,no_of_files bigint,avg_file_size_bytes bigint,avg_file_size_kb double,avg_file_size_mb double,avg_file_size_gb double,table_size bigint)"
    echo "mysql load HIVE_META_HDFS_LOCATION_MAPPING csv to HIVE_META_HDFS_LOCATION_MAPPING table "
    runMysqlQuery "LOAD DATA LOCAL INFILE \"$hiveTableLoactionMapping\" INTO TABLE  hive_metrics.HIVE_META_HDFS_LOCATION_MAPPING FIELDS TERMINATED BY '^' LINES TERMINATED BY '\n'"
    echo "mysql update table_size column in HIVE_META_HDFS_LOCATION_MAPPING"
    runMysqlQuery "use hive_metrics;update HIVE_META_HDFS_LOCATION_MAPPING M INNER JOIN (SELECT SUM(location_size)as tbl_size,tbl_id FROM HIVE_META_HDFS_LOCATION_MAPPING GROUP BY tbl_id) M1 ON M.tbl_id=M1.tbl_id SET table_size=M1.tbl_size"
    echo "mysql update all_table_locations_exists column"
    runMysqlQuery "use hive_metrics;update HIVE_META_HDFS_LOCATION_MAPPING set all_table_locations_exists='YES'"
    runMysqlQuery "use hive_metrics;update HIVE_META_HDFS_LOCATION_MAPPING a join (select distinct tbl_id from HIVE_META_HDFS_LOCATION_MAPPING where location_exists='NO')b on a.tbl_id=b.tbl_id set a.all_table_locations_exists='NO'"
    echo "creating table  TABLE_CATEGORY"
    runMysqlQuery "use hive_metrics;CREATE TABLE TABLE_CATEGORY (NAME varchar(5) DEFAULT NULL,VALUE varchar(5) DEFAULT NULL);insert into TABLE_CATEGORY(NAME,VALUE) values('TINY','TINY'),('BIG','BIG'),('SMALL','SMALL'),('ALL','%')"
    echo "creating data_load_on table"
    runMysqlQuery "use hive_metrics;create table data_load_on (today_date varchar(12));insert into data_load_on values(\"$today\")"
#    runMysqlQuery "LOAD DATA LOCAL INFILE \"$hiveTableLoactionMapping\" INTO TABLE  hive_metrics.HIVE_META_HDFS_LOCATION_MAPPING  FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'"
        #$today/"HIVE_META_HDFS_LOCATION_MAPPING_dump_"$today
    echo "creating indexes"
    runMysqlQuery "use hive_metrics;create index index_tbl_id on HIVE_META_HDFS_LOCATION_MAPPING(tbl_id);create index index_tbl_id on HIVE_TABLES_LAST_ACCESS(tbl_id);create index index_tbl_id on TBLS(tbl_id);"
    echo "load dashboard tables load complete"
 
}
 
echo "deleting old data folders if existed"
 
find . -name '202*' -type d -mtime +1 -exec rm -rf {} \;
 

getInputFiles
decodeFsimage
tablesLastAccess &
#decodeFsimage
createFsimageTables
wait
tableLocationMapping
loadDashboardTables
 
echo "fsimage decoded and uploaded to hdfs "
echo "last Access data retreived"
echo "deleting data folder older than 1 day"
if
 