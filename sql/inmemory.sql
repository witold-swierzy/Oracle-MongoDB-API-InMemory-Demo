create json collection table JSONFLIGHTS_COL_LARGE

begin
dbms_cloud.copy_collection(collection_name => 'JSONFLIGHTS_COL_LARGE', 
                           credential_name => 'ajd_cred', 
                           file_uri_list   => 'https://objectstorage.eu-frankfurt-1.oraclecloud.com/n/fro8fl9kuqli/b/json_demo/o/flights-150m.json', 
                           format => json_object('recorddelimiter' value '''\n'''));
end;
/

alter table jsonflights_col_large noparallel no inmemory;

exec dbms_stats.gather_table_stats('ORADEV','JSONFLIGHTS_COL_LARGE')

alter session set optimizer_ignore_hints=false;

alter table jsonflights_col_large noparallel no inmemory;

select count(*) from jsonflights_col_large;

alter table jsonflights_col_large parallel 16;

select count(*) from jsonflights_col_large;

alter table jsonflights_col_large inmemory priority critical duplicate;

select owner, 
       segment_name, 
       inmemory_size, 
       bytes, 
       bytes_not_populated, 
       populate_status
from v$im_segments;

select count(*) from jsonflights_col_large;

alter table jsonflights_col_large inmemory cellmemory;

alter table jsonflights_col_large inmemory priority critical duplicate;






