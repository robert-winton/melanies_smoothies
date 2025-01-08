-- Badge1_2024-12-15.sql

select 'hello';
select 'hello' as "Greeting";

SHOW DATABASES;
SHOW SCHEMAS;

SHOW SCHEMAS IN ACCOUNT;

-- Create Root depth

create or replace table ROOT_DEPTH (
   ROOT_DEPTH_ID number(1), 
   ROOT_DEPTH_CODE text(1), 
   ROOT_DEPTH_NAME text(7), 
   UNIT_OF_MEASURE text(2),
   RANGE_MIN number(2),
   RANGE_MAX number(2)
   ); 

insert into root_depth 
values
(
    1,
    'S',
    'Shallow',
    'cm',
    30,
    45
)
;

insert into root_depth 
values
(
    2,
    'M',
    'Medium',
    'cm',
    45,
    60
)
;

insert into root_depth 
values
(
    3,
    'D',
    'Deep',
    'cm',
    60,
    90
)
;

select * 
from garden_plants.information_schema.schemata;

SELECT * 
FROM GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
where schema_name in ('FLOWERS','FRUITS','VEGGIES');

select count(*) as schemas_found, '3' as schemas_expected 
from GARDEN_PLANTS.INFORMATION_SCHEMA.SCHEMATA
where schema_name in ('FLOWERS','FRUITS','VEGGIES');

create table garden_plants.veggies.vegetable_details
(
plant_name varchar(25)
, root_depth_code varchar(1)    
);

-- 2024-12-15 7:48pm

create or replace TABLE SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST (
	RAW_STATUS VARIANT
);

create file format SOCIAL_MEDIA_FLOODGATES.PUBLIC.json_file_format
type = 'JSON' 
compression = 'AUTO' 
enable_octal = FALSE
allow_duplicate = FALSE
strip_outer_array = TRUE
strip_null_values = FALSE
ignore_utf8_errors = FALSE;


select $1
from @SOCIAL_MEDIA_FLOODGATES.PUBLIC.SOCIALMEDIASTAGE/nutrition_tweets.json
(file_format => SOCIAL_MEDIA_FLOODGATES.PUBLIC.json_file_format );

copy into TWEET_INGEST
from @SOCIAL_MEDIA_FLOODGATES.PUBLIC.SOCIALMEDIASTAGE
files = ( 'nutrition_tweets.json')
file_format = ( format_name=SOCIAL_MEDIA_FLOODGATES.PUBLIC.json_file_format );

select * from TWEET_INGEST;

----simple select statements ---- are you seeing 9 rows?
select raw_status
from tweet_ingest;

select raw_status:entities
from tweet_ingest;

select raw_status:entities:hashtags
from tweet_ingest;

----Explore looking at specific hashtags by adding bracketed numbers
----This query returns just the first hashtag in each tweet
select raw_status:entities:hashtags[0].text
from tweet_ingest;

----This version adds a WHERE clause to get rid of any tweet that 
----doesn't include any hashtags
select raw_status:entities:hashtags[0].text
from tweet_ingest
where raw_status:entities:hashtags[0].text is not null;

----Perform a simple CAST on the created_at key
----Add an ORDER BY clause to sort by the tweet's creation date
select raw_status:created_at::date
from tweet_ingest
order by raw_status:created_at::date;

select value
from tweet_ingest
,lateral flatten
(input => raw_status:entities:urls);

select value
from tweet_ingest
,table(flatten(raw_status:entities:urls));


----Flatten and return just the hashtag text, CAST the text as VARCHAR
select value:text::varchar as hashtag_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:hashtags);

----Add the Tweet ID and User ID to the returned table so we could join the hashtag back to it's source tweet
select raw_status:user:name::text as user_name
,raw_status:id as tweet_id
,value:text::varchar as hashtag_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:hashtags);


create or replace view social_media_floodgates.public.urls_normalized as
(select raw_status:user:name::text as user_name
,raw_status:id as tweet_id
,value:display_url::text as url_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:urls)
);

select * from social_media_floodgates.public.urls_normalized;

create or replace view social_media_floodgates.public.hashtags_normalized as
(select raw_status:user:name::text as user_name
,raw_status:id as tweet_id
,value:text::text as hashtag_used
from tweet_ingest
,lateral flatten
(input => raw_status:entities:hashtags)
);

select * from social_media_floodgates.public.hashtags_normalized;

-- 2024-12-15 6:59pm

create sequence SEQ_AUTHOR_UID
     start = 1
     increment = <integer>
     ORDER
     comment = 'Use this to fill in AUTHOR_UID';

-- Fruit Details

create or replace TABLE GARDEN_PLANTS.FRUITS.FRUIT_DETAILS (
	PLANT_NAME VARCHAR(25),
	ROOT_DEPTH_CODE VARCHAR(1)
);

select * from GARDEN_PLANTS.FRUITS.FRUIT_DETAILS;

-- 2024-12-15 4:50pm

create or replace table vegetable_details_soil_type
( plant_name varchar(25)
 ,soil_type number(1,0)
);

create file format garden_plants.veggies.PIPECOLSEP_ONEHEADROW 
    type = 'CSV'--csv is used for any flat file (tsv, pipe-separated, etc)
    field_delimiter = '|' --pipes as column separators
    skip_header = 1 --one header row to skip
    ;

copy into vegetable_details_soil_type
from @util_db.public.my_internal_stage
files = ( 'VEG_NAME_TO_SOIL_TYPE_PIPE.txt')
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.PIPECOLSEP_ONEHEADROW );

create file format garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW 
    TYPE = 'CSV'--csv for comma separated files
    FIELD_DELIMITER = ',' --commas as column separators
    SKIP_HEADER = 1 --one header row  
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' --this means that some values will be wrapped in double-quotes bc they have commas in them
    ;

    --The data in the file, with no FILE FORMAT specified
select $1
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv;

--Same file but with one of the file formats we created earlier  
select $1, $2, $3
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.COMMASEP_DBLQUOT_ONEHEADROW);

--Same file but with the other file format we created earlier
select $1, $2, $3
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.PIPECOLSEP_ONEHEADROW );


create file format garden_plants.veggies.L9_CHALLENGE_FF 
    TYPE = 'CSV'
    FIELD_DELIMITER = '\t' --commas as column separators
    SKIP_HEADER = 1 --one header row  
    ----FIELD_OPTIONALLY_ENCLOSED_BY = '"' --this means that some values will be wrapped in double-quotes bc they have commas in them
    ;

select $1, $2, $3
from @util_db.public.my_internal_stage/LU_SOIL_TYPE.tsv
(file_format => garden_plants.veggies.L9_CHALLENGE_FF );

create or replace table LU_SOIL_TYPE(
SOIL_TYPE_ID number,	
SOIL_TYPE varchar(15),
SOIL_DESCRIPTION varchar(75)
 );


copy into LU_SOIL_TYPE
from @util_db.public.my_internal_stage
files = ( 'LU_SOIL_TYPE.tsv')
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.L9_CHALLENGE_FF );

select * from LU_SOIL_TYPE;

create or replace table VEGETABLE_DETAILS_PLANT_HEIGHT(
plant_name varchar(18),
UOM varchar(2),
Low_End_of_Range number,
High_End_of_Range number
----SOIL_TYPE_ID number,	
----SOIL_TYPE varchar(15),
----SOIL_DESCRIPTION varchar(75)
 );


copy into VEGETABLE_DETAILS_PLANT_HEIGHT
from @util_db.public.my_internal_stage
files = ( 'veg_plant_height.csv')
file_format = ( format_name=GARDEN_PLANTS.VEGGIES.COMMASEP_DBLQUOT_ONEHEADROW );

select * from VEGETABLE_DETAILS_PLANT_HEIGHT;

-- 2024-12-15 6:42pm

use role sysadmin;

---- Create a new database and set the context to use the new database
create database library_card_catalog comment = 'DWW Lesson 10 ';

----Set the worksheet context to use the new database
use database library_card_catalog;

create or replace table book
( book_uid number autoincrement
 , title varchar(50)
 , year_published number(4,0)
);

insert into book(title, year_published)
values
 ('Food',2001)
,('Food',2006)
,('Food',2008)
,('Food',2016)
,('Food',2015);

select * from book;

create or replace table author (
   author_uid number 
  ,first_name varchar(50)
  ,middle_name varchar(50)
  ,last_name varchar(50)
);

---- Insert the first two authors into the Author table
insert into author(author_uid, first_name, middle_name, last_name)  
values
(1, 'Fiona', '','Macdonald')
,(2, 'Gian','Paulo','Faleschini');

---- Look at your table with it's new rows
select * 
from author;

use role sysadmin;

----See how the nextval function works
select seq_author_uid.nextval;

-- 2024-12-15 4:47pm

create or replace TABLE FLOWER_DETAILS (
	PLANT_NAME VARCHAR(25),
	ROOT_DEPTH_CODE VARCHAR(1)
);

-- DWW Lesson 10

create sequence SEQ_AUTHOR_UID
     start = 1
     increment = 1
     ORDER
     comment = 'Use this to fill in AUTHOR_UID';

use role sysadmin;

----See how the nextval function works
select seq_author_uid.nextval, seq_author_uid.nextval;

create sequence SEQ_AUTHOR_UID
     start = 1
     increment = 1
     ORDER
     comment = 'Use this to fill in AUTHOR_UID';

show sequences;


create or replace sequence library_card_catalog.public.seq_author_uid
start = 3 
increment = 1 
ORDER
comment = 'Use this to fill in the AUTHOR_UID every time you add a row';

insert into author(author_uid,first_name, middle_name, last_name) 
values
(seq_author_uid.nextval, 'Laura', 'K','Egendorf')
,(seq_author_uid.nextval, 'Jan', '','Grover')
,(seq_author_uid.nextval, 'Jennifer', '','Clapp')
,(seq_author_uid.nextval, 'Kathleen', '','Petelinsek');

create table book_to_author
( book_uid number
  ,author_uid number
);

insert into book_to_author(book_uid, author_uid)
values
 (1,1)  ---- This row links the 2001 book to Fiona Macdonald
,(1,2)  ---- This row links the 2001 book to Gian Paulo Faleschini
,(2,3)  ---- Links 2006 book to Laura K Egendorf
,(3,4)  ---- Links 2008 book to Jan Grover
,(4,5)  ---- Links 2016 book to Jennifer Clapp
,(5,6); ---- Links 2015 book to Kathleen Petelinsek

select * 
from book_to_author ba 
join author a 
on ba.author_uid = a.author_uid 
join book b 
on b.book_uid=ba.book_uid;

create table library_card_catalog.public.author_ingest_json
(
  raw_author variant
);

create file format library_card_catalog.public.json_file_format
type = 'JSON' 
compression = 'AUTO' 
enable_octal = FALSE
allow_duplicate = FALSE
strip_outer_array = TRUE
strip_null_values = FALSE
ignore_utf8_errors = FALSE;

select $1
from @LIBRARY_CARD_CATALOG.PUBLIC.LIBRARYSTAGE/author_with_header.json
(file_format => LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT );

copy into AUTHOR_INGEST_JSON
from @LIBRARY_CARD_CATALOG.PUBLIC.LIBRARYSTAGE
files = ( 'author_with_header.json')
file_format = ( format_name=LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT );

select raw_author from author_ingest_json;

----returns AUTHOR_UID value from top-level object's attribute
select raw_author:AUTHOR_UID
from author_ingest_json;

----returns the data in a way that makes it look like a normalized table
SELECT 
 raw_author:AUTHOR_UID
,raw_author:FIRST_NAME::STRING as FIRST_NAME
,raw_author:MIDDLE_NAME::STRING as MIDDLE_NAME
,raw_author:LAST_NAME::STRING as LAST_NAME
FROM AUTHOR_INGEST_JSON;

select GRADER(step, (actual = expected), actual, expected, description) as graded_results from
(
  SELECT 'DWW16' as step
  ,( select row_count 
    from LIBRARY_CARD_CATALOG.INFORMATION_SCHEMA.TABLES 
    where table_name = 'AUTHOR_INGEST_JSON') as actual
  ,6 as expected
  ,'Check number of rows' as description
 );

select $1
from @LIBRARY_CARD_CATALOG.PUBLIC.LIBRARYSTAGE/json_book_author_nested.txt
(file_format => LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT );

copy into NESTED_INGEST_JSON
from @LIBRARY_CARD_CATALOG.PUBLIC.LIBRARYSTAGE
files = ( 'json_book_author_nested.txt')
file_format = ( format_name=LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT );

select raw_nested_book
from nested_ingest_json;

select raw_nested_book:year_published
from nested_ingest_json;

select raw_nested_book:authors
from nested_ingest_json;

----Use these example flatten commands to explore flattening the nested book and author data
select value:first_name
from nested_ingest_json,
lateral flatten(input => raw_nested_book:authors);

select value:first_name
from nested_ingest_json
,table(flatten(raw_nested_book:authors));

----Add a CAST command to the fields returned
SELECT value:first_name::varchar, value:last_name::varchar
from nested_ingest_json
,lateral flatten(input => raw_nested_book:authors);

----Assign new column  names to the columns using "AS"
select value:first_name::varchar as first_nm
, value:last_name::varchar as last_nm
from nested_ingest_json
,lateral flatten(input => raw_nested_book:authors);



