create file format smoothies.public.two_headerrow_pct_delim
   type = CSV,
   skip_header = 2,   
   field_delimiter = '%',
   trim_space = TRUE
;

SELECT $1, $2
FROM @SMOOTHIES.PUBLIC.MY_UPLOADED_FILES/fruits_available_for_smoothies.txt
(FILE_FORMAT => smoothies.public.two_headerrow_pct_delim);

COPY INTO smoothies.public.fruit_options
from (select $2 as fruit_id, $1 as fruit_name from @SMOOTHIES.PUBLIC.MY_UPLOADED_FILES/fruits_available_for_smoothies.txt)
file_format = (format_name = smoothies.public.two_headerrow_pct_delim)
on_error = abort_statement
purge = true;

create table smoothies.public.ORDERS(
ingredients varchar(200)
);

select * from smoothies.public.orders;

truncate table smoothies.public.orders;

alter table smoothies.public.orders add column ORDER_FILLED BOOLEAN DEFAULT FALSE;

insert into smoothies.public.orders(ingredients, name_on_order) values ('Dragon Fruit Elderberries Figs Cantaloupe Guava ' ,'Robert Winton');

update smoothies.public.orders
       set order_filled = true
       where name_on_order is null;

alter table SMOOTHIES.PUBLIC.ORDERS 
add column order_uid integer --adds the column
default smoothies.public.order_seq.nextval  --sets the value of the column to sequence
constraint order_uid unique enforced; --makes sure there is always a unique value in the column

create or replace table smoothies.public.orders (
       order_uid integer default smoothies.public.order_seq.nextval,
       order_filled boolean default false,
       name_on_order varchar(100),
       ingredients varchar(200),
       constraint order_uid unique (order_uid),
       order_ts timestamp_ltz default current_timestamp()
);

alter table SMOOTHIES.PUBLIC.FRUIT_OPTIONS add column SEARCH_ON VARCHAR(200);

select * from SMOOTHIES.PUBLIC.FRUIT_OPTIONS;

update SMOOTHIES.PUBLIC.FRUIT_OPTIONS
set search_on = Fruit_name;

update SMOOTHIES.PUBLIC.FRUIT_OPTIONS
set Search_on = 'Apple'
WHERE Fruit_name = 'Apples';

update SMOOTHIES.PUBLIC.FRUIT_OPTIONS
set Search_on = 'Blueberry'
WHERE Fruit_name = 'Blueberries';

update SMOOTHIES.PUBLIC.FRUIT_OPTIONS
set Search_on = 'Jackfruit'
WHERE Fruit_name = 'Jackfruit';

update SMOOTHIES.PUBLIC.FRUIT_OPTIONS
set Search_on = 'Raspberry'
WHERE Fruit_name = 'Raspberries';

update SMOOTHIES.PUBLIC.FRUIT_OPTIONS
set Search_on = 'Strawberry'
WHERE Fruit_name = 'Strawberries';

update SMOOTHIES.PUBLIC.FRUIT_OPTIONS
set Search_on = 'Dragonfruit'
WHERE Fruit_name = 'Dragon Fruit';

