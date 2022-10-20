create database servier;
use servier;

create table if not exists transactions(
    date date not null,
    order_id integer not null,
    client_id integer not null,
    prod_id varchar(30) not null,
    prod_price float not null,
    prod_qty float not null
);
-- Load data from transactions.csv to 
LOAD DATA INFILE '/var/lib/mysql-files//transactions.csv'
INTO TABLE transactions
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 ROWS 
(@date, order_id, client_id, prod_id, prod_price, prod_qty)
SET date = STR_TO_DATE(@date, '%d-%m-%Y');


create table if not exists produits(
    product_id integer not null primary key,
    product_type varchar(50) not null,
    product_name varchar(60) not null
);

insert into produits(product_id, product_type, product_name)
values(490756, 'MEUBLE', ' Chaise');

insert into produits(product_id, product_type, product_name)
values(389728, 'DECO', 'Boule de Noël');

insert into produits(product_id, product_type, product_name)
values(549380, 'MEUBLE', ' Canapé');

insert into produits(product_id, product_type, product_name)
values(293718, 'DECO', ' Mug');

-- first query
select date, sum(ROUND(prod_price * prod_qty, 2)) as ventes
    from transactions
    where date between '2020-01-01' and '2020-12-31'
    group by date;


select client_id, 
                sum(case when product_type = 'MEUBLE'  then ROUND(prod_price * prod_qty, 2) else 0 end) as ventes_meubles,
                sum(case when product_type = 'DECO'  then ROUND(prod_price * prod_qty, 2) else 0 end) as ventes_deco
                
from transactions
join produits
where transactions.prod_id = produits.product_id and date between '2020-01-01' and '2020-12-31'
group by client_id
;

--  