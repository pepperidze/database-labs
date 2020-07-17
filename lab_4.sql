create extension tablefunc;
create table cars (
    id                      serial not null primary key,        	
    car_brand               varchar(40) not null,
    production_date		    date not null,
    commissioning_date	    date not null
);
create table routes (
    id					    serial not null primary key,        	        	
    route_code			    varchar(40) not null,
    loading_place		    varchar(40) not null,
    destination_place	    varchar(40) not null
);
create table drivers (
    id					    serial not null primary key,        	       	
    driver_number		    varchar(40) not null,
    full_name			    varchar(40) not null,
    employment_date		    date not null
);
create table plan (
    id					    serial not null primary key,        			
    driver_id			    integer references drivers (id),	
    transfer_date		    date not null,
    route_id			    integer references routes (id),
    car_id				    integer references cars (id)
);
insert into routes (route_code, loading_place, destination_place) values
    ('КВ-01', 'Київ', 'Луцьк'),
    ('КВ-02', 'Київ', 'Харків'),
    ('КВ-03', 'Луцьк', 'Харків'),
    ('КВ-04', 'Київ', 'Харків'),
    ('КВ-05', 'Кропивницький', 'Луцьк'),
    ('КВ-06', 'Одеса', 'Харків'),
    ('КВ-07', 'Львів', 'Луцьк'),
    ('КВ-08', 'Суми', 'Харків'),
    ('КВ-09', 'Київ', 'Одеса'),
    ('КВ-10', 'Кропивницький', 'Харків');
insert into cars (car_brand, production_date, commissioning_date) values
    ('Aston Martin', '2001-06-11', '2001-09-15'),
    ('Daewoo', '2005-03-22', '2006-01-01'),
    ('Honda', '2011-09-23', '2011-11-02'),
    ('Opel', '2007-02-07', '2012-04-15');
insert into drivers (driver_number, full_name, employment_date) values
    ('ТН-734901', 'Петров І.С.', '2004-04-12'),
    ('ТН-234287', 'Бондаренко Є.А.', '2007-08-18'),
    ('ТН-763189', 'Іванчук С.П.', '2008-01-30'),
    ('ТН-047623', 'Кравець О.О.', '2009-03-23');
insert into plan (driver_id, transfer_date, route_id, car_id) values
    (1, '2015-01-01', 1, 1),
    (1, '2015-01-02', 2, 1), 
    (1, '2015-01-03', 3, 1),
    (1, '2015-01-04', 4, 2),

    (1, '2016-01-01', 1, 3),
    (1, '2016-01-02', 2, 4),
    (1, '2016-01-03', 3, 4),
    (1, '2016-01-04', 4, 4),

    (2, '2016-02-01', 1, 1),
    (2, '2016-02-02', 2, 2), 
    (2, '2016-02-03', 3, 2),
    (2, '2016-02-04', 4, 3),

    (2, '2017-02-01', 1, 3),
    (2, '2017-02-02', 2, 4),
    (3, '2017-03-01', 3, 2),
    (3, '2017-03-02', 4, 3),

    (4, '2017-04-01', 1, 1),
    (4, '2017-04-02', 2, 4);

-- task 1.1 Для кожного водія підрахувати кількість перевезень в залежності від марки авто.
-- task 1.1
select * from crosstab (
'select driver_id, car_id, count(*)::integer from plan
group by 1, 2 order by 1,2',
$$values ('1'), ('2'), ('3'), ('4')$$) 
as plan ("driver_id" integer, "car1" integer, "car2" integer, "car3" integer, "car4" integer);

-- task 1.2 Для найпопулярніших маршрутів підрахувати кількість перевезень кожним водієм, 
-- (найпопулярніші маршрути визначаються по максимальній кількості перевезень). 
-- task 1.2
select * from crosstab (
'with cte as (
select route_id, count (*) as cnt from plan 
group by route_id
)
select cte.route_id, plan.driver_id, count(*)::integer from cte
inner join plan
on plan.route_id = cte.route_id
where cnt = (select max(cnt) from cte)
group by 1, 2 
order by 1,2',
$$values ('1'), ('2'), ('3'), ('4')$$) 
as plan ("route_id" integer, "dr1" integer, "dr2" integer, "dr3" integer, "dr4" integer);

-- task 1.3 Реформувати таблицю так, щоб для кожного перевезення виводилися код маршруту, ціна поїздки та водій.
-- task 1.3 
create table mytab (
    route_id    integer, 
    driver1     integer not null, 
    driver2     integer not null,
    driver3     integer not null,
    driver4     integer not null
);
insert into mytab (route_id, driver1, driver2, driver3, driver4) values
    (1, 100, 200, 300, 400),
    (2, 500, 300, 400, 700),
    (3, 200, 100, 800, 300);
select route_id, driver1 as price, 'driver1' as driver
from mytab
union all
select route_id, driver2, 'driver2'
from mytab
union all
select route_id, driver3, 'driver3'
from mytab
union all
select route_id, driver4, 'driver4'
from mytab
order by route_id, driver;