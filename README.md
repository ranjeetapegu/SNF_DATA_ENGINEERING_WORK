This repository contains simple sample worksheet and programs that will make snowflake Data Engineering works easy for you.

Initial Task :-
1. Get a snowflake account with accountadmin or appropriate previleges.
2. Create DB, schema and stages
   
// create DB
create database SNF_RPEGU_DEMO_DB;

// create schema
create schema snf_rpegu_demo_db.data;

use role accountadmin;
use database SNF_RPEGU_DEMO_DB;
use schema snf_rpegu_demo_db.data;

//creating stage
create stage if not exists
stg_unstructured 
ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE') DIRECTORY = ( ENABLE = true );

//upload  the excel file 
list @stg_unstructured ;
