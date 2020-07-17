// task 1
db.getCollection('plan').update( 
    {
        "_id" : ObjectId("5a2c1ca1f450536212d8fb7b")
    },
    {
        $set : {drivers : [{"driver" : "Петров І.С.", "employ_date" : "2004-04-12"}, 
                           {"driver" : "Бондаренко Є.А.", "employ_date" : "2007-08-18"}],
                route : {"route_code" : "КВ-01", "town_from" : "Київ", "town_to" : "Луцьк"},
                cars : [{"car_brand" : "Aston Martin", "production_date" : "2001-06-11", "commissioning_date" : "2011-09-15"},
                        {"car_brand" : "Opel", "production_date" : "2001-06-11", "commissioning_date" : "2010-01-01"}]
        }   
    }
);
    
db.getCollection('plan').update( 
    {
        "_id" : ObjectId("5a2c1ca1f450536212d8fb79")
    },
    {
        $set : {drivers : [{"driver" : "Іванчук С.П.", "employ_date" : "2008-01-30"}, 
                           {"driver" : "Кравець О.О.", "employ_date" : "2009-03-23"}],
                route : {"route_code" : "КВ-02", "town_from" : "Київ", "town_to" : "Харків"},
                cars : [{"car_brand" : "Daewoo", "production_date" : "2005-03-22", "commissioning_date" : "2010-01-01"},
                        {"car_brand" : "Honda", "production_date" : "2009-09-23", "commissioning_date" : "2012-04-15"}]
        }
    }
);    

db.getCollection('plan').update( 
    {
        "_id" : ObjectId("5a2c1ca1f450536212d8fb7a")
    },
    {
        $set : {drivers : [{"driver" : "Іванчук С.П.", "employ_date" : "2008-01-30"}],
                route: {"route_code" : "КВ-03", "town_from" : "Луцьк", "town_to" : "Харків"},
                cars : [{"car_brand" : "Aston Martin", "production_date" : "2009-09-23", "commissioning_date" : "2010-01-01"},
                        {"car_brand" : "Aston Martin", "production_date" : "2009-09-23", "commissioning_date" : "2011-11-02"},
                        {"car_brand" : "Honda", "production_date" : "2005-03-22", "commissioning_date" : "2010-01-01"}]
        }
    }
);

db.getCollection('plan').update( 
    {
        "_id" : ObjectId("5a2c1ca1f450536212d8fb7c")
    },
    {
        $set : {drivers : [{"driver" : "Кравець О.О.", "employ_date" : "2009-03-23"}],
                route : {"route_code" : "КВ-04", "town_from" : "Київ", "town_to" : "Харків"},   
                cars : [{"car_brand" : "Opel", "production_date" : "2007-02-07", "commissioning_date" : "2011-11-02"}, 
                        {"car_brand" : "Aston Martin", "production_date" : "2001-06-11", "commissioning_date" : "2011-09-15"}]
        }
    }
);    
    
db.getCollection('plan').update( 
    {
        "_id" : ObjectId("5a2c1ca1f450536212d8fb7d")
    },
    {
        $set : {drivers : [{"driver" : "Петров І.С.", "employ_date" : "2004-04-12"}, 
                           {"driver" : "Кравець О.О.", "employ_date" : "2009-03-23"}],
                route :   {"route_code" : "КВ-02", "town_from" : "Київ", "town_to" : "Харків"},
                cars : [{"car_brand" : "Opel", "production_date" : "2001-06-11", "commissioning_date" : "2010-01-01"},
                        {"car_brand" : "Honda", "production_date" : "2009-09-23", "commissioning_date" : "2012-04-15"}]
        }
    }
);

// task 2.1 -- aggregate
db.plan.aggregate( [
    { $unwind: "$drivers" },
    { $match: { 'drivers.driver': 'Петров І.С.' } },
    {
        $group: {
            _id: null,
            total: { $sum: "$price" }
        }
    }
] );

// task 2.2 -- aggregate
db.plan.aggregate( [
    { $unwind: "$cars" },
    { $match: { 'cars.car_brand': 'Opel' } },
    {
        $group: {
            _id: null,
            count: { $sum: 1 }
        }
    }
] );

// task 2.3 -- group 
db.plan.aggregate( [
    { $unwind: "$drivers" },
    {
        $group: {
            _id: "$drivers.driver",
            total: { $sum: "$price" }
        }
    }
] );

// task 2.4 -- group
db.plan.aggregate( [
    { $unwind: "$cars" },
    {
        $group: {
            _id: "$cars.car_brand",
            count: { $sum: 1 }           
        }
    },
    { $sort: {count: 1} },
    { $limit: 1}
] );

// task 3.1
db.plan.aggregate([
   {$unwind: "$route_id"},
   {
     $lookup:
       {
         from: "routes",
         localField: "route_id",
         foreignField: "id",
         as: "route_info"
       }
  },
  {$match: { "route_info": { $ne: [] } }},
  {$match: { 'route_info.kilometrage': {$gt: 150} }},
]);

// task 3.2
var map_plan = function () {
    var output= {route_id : this.route_id, road_number : this.road_number, duration : null, kilometrage : null}
    emit(this.route_id, output);
};

var map_routes = function () {
    var output= {route_id : this.id, road_number : null, duration : this.duration, kilometrage : this.kilometrage}
    emit(this.id, output);
};

var reduceF = function(key, values) {
    var outs = { road_number : null, duration : null, kilometrage : null };
    
    values.forEach(function(v){
        if(outs.road_number == null){
            outs.road_number = v.road_number
        }
        if(outs.duration == null){
            outs.duration = v.duration
        }
        if(outs.kilometrage == null){
            outs.kilometrage = v.kilometrage
        }
    });
    return outs;
};
result = db.plan.mapReduce(map_plan, reduceF, {out: {reduce: 'joined'}})
result = db.routes.mapReduce(map_routes,reduceF, {out: {reduce: 'joined'}});
db.joined.find();





// update date
db.plan.find({created_at: {$not: {$type: 9}}}).forEach(function(doc) {
    // Convert created_at to a Date 
    doc.transfer_date = new Date(doc.transfer_date);
    db.plan.save(doc);
})
