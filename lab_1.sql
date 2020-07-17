-- task 1
-- task 1.1 Підрахувати кількість перевезень для кожного водія, вивести в алфавітному порядку.
-- 1.1.1
select distinct drivers.full_name, drivers.id, count(plan.driver_id) over (partition by drivers.id) from drivers
inner join plan
on drivers.id = plan.driver_id
order by drivers.full_name;
-- 1.1.2
select drivers.full_name, drivers.id, count(plan.driver_id) from drivers
inner join plan
on drivers.id = plan.driver_id
group by drivers.id
order by drivers.full_name;

-- task 1.2 Пронумерувати перевезення за датою для кожного водія.
-- 1.2.1
select drivers.full_name, drivers.id, plan.transfer_date, rank() over (partition by drivers.id order by plan.transfer_date) from drivers
inner join plan
on drivers.id = plan.driver_id
order by drivers.id;
-- 1.2.2
select drivers.full_name, drivers.id, plan.transfer_date,
(select count(*) from plan pp where pp.driver_id = plan.driver_id and pp.transfer_date < plan.transfer_date) + 1 as date_rank
from drivers
inner join plan
on drivers.id = plan.driver_id
order by drivers.id, date_rank;

-- task 1.3 Для кожної поточного перевезення(в межах одного водія) підрахувати середню вартість поїздки 
-- на основі поточної та попередньої поїздок. Відсортувати за датою.
-- update table
alter table plan add column price real;
update plan set price = 1000  where id = 1;
update plan set price = 2000  where id = 2;
update plan set price = 3000  where id = 3;
update plan set price = 4000  where id = 4;
update plan set price = 5000  where id = 5;
update plan set price = 6000  where id = 6;
update plan set price = 7000  where id = 7;
update plan set price = 8000  where id = 8;
update plan set price = 9000  where id = 9;
update plan set price = 9000  where id = 10;
update plan set price = 8000  where id = 11;
update plan set price = 7000  where id = 12;
-- 1.3.1
select driver_id, transfer_date, price, 
avg(price) over (partition by driver_id order by transfer_date rows 1 preceding) as avg_2
from plan;
-- 1.3.2
with tmp as
(select p1.price, count(p1.transfer_date) as param, p1.driver_id, p1.transfer_date
from plan as p1, plan as p2
where p1.transfer_date >= p2.transfer_date and p1.driver_id = p2.driver_id
group by p1.id
order by p1.transfer_date)
select tmp1.driver_id, tmp1.transfer_date, tmp1.price, avg(tmp2.price) as avg_2
from tmp as tmp1, tmp as tmp2
where tmp2.param in (tmp1.param, tmp1.param - 1) and tmp1.driver_id = tmp2.driver_id
group by tmp1.transfer_date, tmp1.driver_id, tmp1.price
order by tmp1.driver_id;

-- task 1.4 Для кожної поточного перевезення(в межах одного водія) підрахувати середню вартість поїздки 
-- на основі поточної та всіх попередніх поїздок. Відсортувати за датою.
-- 1.4.1
select driver_id, transfer_date, price, 
floor(avg(price) over (partition by driver_id order by transfer_date range between unbounded preceding and current row)) 
as avg_price
from plan;
-- 1.4.2
with tmp as (select driver_id, transfer_date, price from plan)
select tmp1.driver_id, tmp1.transfer_date, tmp1.price, floor(avg(tmp2.price)) as avg_price
from tmp as tmp1, tmp as tmp2
where tmp1.transfer_date >= tmp2.transfer_date and tmp2.driver_id = tmp1.driver_id
group by tmp1.driver_id, tmp1.transfer_date, tmp1.price
order by tmp1.driver_id, tmp1.transfer_date;

-- task 1.5
-- Для кожного поточного перевезення(в межах одного водія) вивести дату попереднього перевезення. Відсортувати за датою.
-- 1.5.1
select driver_id, transfer_date, 
lag(transfer_date) over (partition by driver_id order by transfer_date) as pre_date 
from plan;
-- 1.5.2
with tmp as
(
    select p1.id, p1.driver_id, p1.transfer_date, count(p2.id) as row_id
    from plan as p1, plan as p2
    where (p1.transfer_date >= p2.transfer_date) and p1.driver_id = p2.driver_id
    group by p1.driver_id, p1.id, p1.transfer_date
)
select tmp1.driver_id, tmp1.transfer_date, tmp2.transfer_date as pre_date
from tmp as tmp1
left join  tmp as tmp2
on tmp2.row_id = tmp1.row_id - 1 and tmp1.driver_id = tmp2.driver_id
order by tmp1.driver_id;

-- task 1.6
-- Для кожного поточного перевезення(в межах одного водія) вивести дату наступного перевезення. Відсортувати за датою.
-- 1.6.1
select driver_id, transfer_date, 
lead(transfer_date) over (partition by driver_id order by transfer_date) as foll_date 
from plan;
-- 1.6.2
with tmp as
(
    select p1.id, p1.driver_id, p1.transfer_date, count(p2.id) as row_id
    from plan as p1, plan as p2
    where (p1.transfer_date >= p2.transfer_date) and p1.driver_id = p2.driver_id
    group by p1.driver_id, p1.id, p1.transfer_date
)
select tmp1.driver_id, tmp1.transfer_date, tmp2.transfer_date as pre_date
from tmp as tmp1
left join  tmp as tmp2
on tmp2.row_id = tmp1.row_id + 1 and tmp1.driver_id = tmp2.driver_id
order by tmp1.driver_id;





-- task 2
-- task 2.1
drop table plan, drivers, cars, routes;
create table plan (
    route_code			varchar(40), 	
    driver_name			varchar(40),
    car_model			varchar(40),
    fuel_type			varchar(40),
    price				real
);
insert into plan (route_code, driver_name, car_model, fuel_type, price) values
	('КВ-01', 'Бондар Є.А.', 'Opel', 'diesel', 3000),
	('КВ-05', 'Бондар Є.А.', 'Opel', 'diesel', 1000),
	('КВ-02', 'Кравець О.О.','Honda', 'petrol', 7000),
	('КВ-06', 'Іванчук С.П.', 'Daewoo', 'gas', 5000),
	('КВ-03', 'Кравець О.О.', 'Honda', 'petrol', 4000),
	('КВ-04', 'Іванчук С.П.', 'Daewoo', 'gas', 2000),
	('КВ-01', 'Бондар Є.А.', 'Opel', 'diesel', 3000);
drop table plan;
create table plan (
    route_code			varchar(40) not null primary key, 	
    driver_name			varchar(40),
    car_model			varchar(40),
    fuel_type			varchar(40),
    price				real
);
insert into plan (route_code, driver_name, car_model, fuel_type, price) values
	('КВ-01', 'Бондар Є.А.', 'Opel', 'diesel', 3000),
	('КВ-05', 'Бондар Є.А.', 'Opel', 'diesel', 1000),
	('КВ-02', 'Кравець О.О.','Honda', 'petrol', 7000),
	('КВ-06', 'Іванчук С.П.', 'Daewoo', 'gas', 5000),
	('КВ-03', 'Кравець О.О.', 'Honda', 'petrol', 4000),
	('КВ-04', 'Іванчук С.П.', 'Daewoo', 'gas', 2000);

-- task 2.2
drop table plan;
create table plan_car (
    id					serial not null primary key, 	
    car_model			varchar(40),
    fuel_type			varchar(40)
);
create table plan (
    route_code			varchar(40) not null primary key, 	
    driver_name			varchar(40),
    car_id				int references plan_car (id),
    price				real
);
insert into plan_car (car_model, fuel_type) values
	('Opel', 'diesel'),
	('Honda', 'petrol'),
	('Daewoo', 'gas');
insert into plan (route_code, driver_name, car_id, price) values
	('КВ-01', 'Бондар Є.А.', 1, 3000),
	('КВ-05', 'Бондар Є.А.', 1, 1000),
	('КВ-02', 'Кравець О.О.', 2, 7000),
	('КВ-06', 'Іванчук С.П.', 3, 5000),
	('КВ-03', 'Кравець О.О.', 2, 4000),
	('КВ-04', 'Іванчук С.П.', 3, 2000);

-- task 2.3
drop table plan, plan_car; 
create table plan_car (
    id					serial not null primary key, 	
    car_model			varchar(40),
    fuel_type			varchar(40)
);
create table plan_driver (
    id					serial not null primary key, 	
    driver_name			varchar(40),
    car_id				int references plan_car (id)
);
create table plan (
    route_code			varchar(40) not null primary key, 	
    driver_id			int references plan_driver (id),
    price				real
);
insert into plan_car (car_model, fuel_type) values
    ('Opel', 'diesel'),
    ('Honda', 'petrol'),
	('Daewoo', 'gas');
insert into plan_driver (driver_name, car_id) values
    ('Бондар Є.А.', 1),
    ('Кравець О.О.', 2),
	('Іванчук С.П.', 3);
insert into plan (route_code, driver_id, price) values
	('КВ-01', 1, 3000),
	('КВ-05', 1, 1000),
	('КВ-02', 2, 7000),
	('КВ-06', 3, 5000),
	('КВ-03', 2, 4000),
	('КВ-04', 3, 2000);

-- task 2.4
drop table plan;
create table plan (
    route_code			varchar(40) not null, 	
    driver_id			int references plan_driver (id),
    price				real,
    tariff				varchar(40)
);
insert into plan (route_code, driver_id, price, tariff) values
	('КВ-01', 1, 3000, 'Знижка на КВ-01'),
	('КВ-01', 1, 1000, 'Знижка на КВ-01'),
	('КВ-01', 2, 7000, 'Відсутня знижка на КВ-01'),
	('КВ-02', 3, 5000, 'Відсутня знижка на КВ-02'),
	('КВ-02', 2, 4000, 'Відсутня знижка на КВ-02'),
	('КВ-02', 3, 2000, 'Знижка на КВ-02');
drop table plan;
create table tariffs (
    tariff				varchar(40) not null primary key, 	
    route_code			varchar(40),
    explanation			varchar(40)
);
create table plan (
    tariff				varchar(40) references tariffs (tariff),
    driver_id			int references plan_driver (id),
    price				real
);
insert into tariffs (tariff, route_code, explanation) values
    	('Знижка на КВ-01', 'КВ-01', 'Присутня'),
		('Відсутня знижка на КВ-01', 'КВ-01', 'Відсутня'),
		('Відсутня знижка на КВ-02', 'КВ-01', 'Відсутня'),
    	('Знижка на КВ-02', 'КВ-01', 'Присутня');        
insert into plan (tariff, driver_id, price) values
    	('Знижка на КВ-01', 1, 3000),
    	('Знижка на КВ-01', 1, 1000),
		('Відсутня знижка на КВ-01', 2, 7000),
    	('Відсутня знижка на КВ-02', 3, 5000),
		('Відсутня знижка на КВ-02', 2, 4000),
    	('Знижка на КВ-02', 3, 2000);
