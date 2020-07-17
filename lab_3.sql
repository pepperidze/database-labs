create table drivers (
	id					serial not null primary key,        	       	
  	second_name			varchar(40) not null,
  	first_name			varchar(40) not null,
  	employment_date		date not null
);

create table mentors (
	id					serial not null primary key,
	child_driver		integer references drivers (id),
	parent_driver		integer references drivers (id)
);

insert into drivers (first_name, second_name, employment_date) values
	('Петров', 'Іван', '2004-04-12'),
    ('Бондаренко', 'Євгеній', '2007-08-18'),
    ('Іванчук', 'Сергій', '2008-01-30'),
    ('Кравець', 'Олег', '2009-03-23'),
    ('Коломієць', 'Василь', '2010-05-19'),
    ('Волошин', 'Юрій', '2012-08-15'),
    ('Зоря', 'Анатолій', '2014-02-17'),
    ('Атаманенко', 'Єгор', '2013-12-05'),
    ('Зозуля', 'Степан', '2009-10-11'),
    ('Іващук', 'Роман', '2006-08-22'),
    ('Кравчук', 'Олександр', '2009-03-18'),
    ('Коваль', 'Андрій', '2004-08-03'),
    ('Зубенко', 'Петро', '2012-04-17'),
    ('Тарасенко', 'Тимофій', '2015-01-10'),
    ('Максименко', 'Антон', '2009-11-15');

insert into mentors (child_driver, parent_driver) values
	(4, 1),
	(5, 1),
	(10, 4),
	(11, 4),
	(6, 2),
	(7, 2),
	(12, 6),
	(13, 6),
	(14, 12),
	(15, 12),
	(8, 3),
	(9, 3);

--1) Вивести список всіх «нащадків» вказаного «предка».
with recursive sub (child_driver) as (
	select child_driver from mentors where parent_driver = 2
	union all
	select mentors.child_driver from sub, mentors
	where mentors.parent_driver = sub.child_driver
)
select child_driver from sub;

--2) Вивести список всіх «предків» вказаного «нащадка».
with recursive sub (parent_driver) as (
	select parent_driver from mentors where child_driver = 12
	union all
	select mentors.parent_driver from sub, mentors
	where mentors.child_driver = sub.parent_driver
)
select parent_driver from sub;

--3) Вивести список, другий полем якого є «рівень» (аналог псевдостовпчика level в connect by).
with recursive sub (child_driver, level) as (
	select child_driver, 1 from mentors where parent_driver = 2
	union all
	select mentors.child_driver, sub.level + 1 from sub, mentors
	where mentors.parent_driver = sub.child_driver
)
select * from sub;

--4) ( 2 запити ) Змінити дані в доданій таблиці так, щоб утворився цикл.
--Написати запит, що видає помилку при зациклюванні. Змінити цей запит так, щоб помилки не було.
-- 2.4.1
update mentors
set child_driver = 1 where id = 4;
-- 2.4.2
with recursive sub (child_driver) as (
	select child_driver from mentors where parent_driver = 1
	union all
	select mentors.child_driver from sub, mentors
	where mentors.parent_driver = sub.child_driver
)
select child_driver from sub;
-- 2.4.3
with recursive sub (child_driver, if_loop) as (
	select child_driver, 0 from mentors where parent_driver = 1
	union all
	select mentors.child_driver, (case when sub.child_driver = mentors.parent_driver then 1 else 0 end) as if_loop 
	from sub, mentors
	where mentors.parent_driver = sub.child_driver and sub.if_loop = 0
)
select child_driver from sub;

--5) Для всіх «нащадків» (це перше поле: Іванов ) вивести список «предків» через «/», 
--де останнім в ланцюгу є цей «нащадок» ( це друге поле: Іваненко/Іванченко/Іванчук/Іванов)
with recursive sub (child_driver, chain) as (
	select child_driver, '/' || parent_driver || '/' || child_driver from mentors where parent_driver = 2
	union all
	select mentors.child_driver, sub.chain || '/' || mentors.child_driver from sub, mentors
	where mentors.parent_driver = sub.child_driver
)
select * from sub;
