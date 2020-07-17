-- task 1
with cte as (
	select distinct routes.id as route_id, 
           routes.name as route_name, 
           routes.airport_from as route_from, 
           routes.airport_to as route_to,
           count(tickets.passenger_id) over (partition by routes.id) as numb_on_route
	from routes
	inner join tickets
	on tickets.route_id = routes.id
	inner join passengers
	on tickets.passenger_id = passengers.id
)
select cte.route_id, 
	   cte.route_name, 
	   temp1.name as air_from, 
	   temp2.name as air_to, 
	   cte.numb_on_route,
	   row_number () over (order by cte.numb_on_route desc)
from cte
inner join airports as temp1
on cte.route_from = temp1.id
inner join airports as temp2
on cte.route_to = temp2.id;

-- task 2
with recursive sub (airport_to, if_loop) as (
	select airport_to, 0 from routes where airport_from = 1
	union all
	select routes.airport_to, (case when sub.airport_to = routes.airport_from then 1 else 0 end) as if_loop 
	from sub, routes
	where routes.airport_from = sub.airport_to and sub.if_loop = 0
)
select id, name from sub
inner join airports
on airports.id = sub.airport_to;

-- task 3
with cte as (
	select distinct routes.id, 
           			routes.name as route_name, 
          			routes.rate as route_rate,
         			count(tickets.id) over (partition by routes.id) as numb_on_route
	from routes
	inner join tickets
	on routes.id = tickets.route_id
)
select cte.route_name, cte.route_rate * cte.numb_on_route as route_sum 
from cte
order by route_sum desc 
limit 10;

-- task 4
with cte as (
select distinct routes.id, 
           		routes.name as route_name,
           		count(tickets.passenger_id) over (partition by routes.id) as numb_on_route
	from routes
	inner join tickets
	on routes.id = tickets.route_id
	inner join passengers
	on passengers.id = tickets.passenger_id
	inner join passinfo
	on passinfo.id = tickets.passinfo_id
	where passengers.sex = 'f' and passinfo.seat_class like '%A%'
)
select cte.route_name, cte.numb_on_route from cte
order by cte.numb_on_route desc
limit 3;

-- task 5
with cte as (
	select distinct routes.airport_from as route_from, 
           	routes.airport_to as route_to, 
          	count(tickets.route_id) over (partition by routes.id) as numb_on_route
	from routes
	inner join tickets
	on routes.id = tickets.route_id
)
select distinct temp1.id, 
      			temp1.name, 
	      		sum(cte.numb_on_route) as numb from cte
inner join airports as temp1
on cte.route_from = temp1.id
group by temp1.id
union all
select  temp2.id, 
 		temp2.name, 
	    sum(cte.numb_on_route) as numb from cte
inner join airports as temp2
on cte.route_to = temp2.id
group by temp2.id
order by numb desc;