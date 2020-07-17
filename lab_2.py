import psycopg2
import random, time, math, sys


connect_str = "dbname = 'test_sql' user = 'mvm' host = 'localhost' password = 'mvm'"
conn = psycopg2.connect(connect_str)
conn.autocommit = True

cur = conn.cursor()
print("З'єднання з базою даних успішно створено.")

employment_date_list = ['2010-11-23', '2012-04-05', '2013-02-25', '2011-04-25', '2014-06-19']
first_name_list = ['Іван', 'Петро', 'Юрій', 'Василь', 'Євгеній']
second_name_list = ['Бондаренко', 'Коваленко', 'Іванчук', 'Кравець', 'Ткач']
middle_name_list = ['Олександрович', 'Андрійович', 'Петрович', 'Васильович', 'Володимирович']
road_number_list = ['КВ-01', 'КВ-02', 'КВ-03', 'КВ-04', 'КВ-05']
transfer_date_list = ['2017-01-01', '2017-05-18', '2017-07-30', '2017-10-21', '2017-03-08']


def drop_tables():
    try:
        cur.execute('''drop table drivers, plan;''')
        print("Таблиці були очищені.\n")
    except:
        print("Таблиці вже очищені.\n")


def timing(func):
    def wrapped(*args, **kwargs):
        start_time = time.time()
        func(*args, **kwargs)
        end_time = time.time()
        print('Час виконання: {} s'.format(end_time - start_time))
    return wrapped


def create_tables():
    cur.execute('''
    create table drivers (
        id                serial not null primary key,
        first_name        varchar(16) not null,
        second_name       varchar(16) not null, 
        middle_name       varchar(16) not null,
        employment_date   date not null
    );''')

    cur.execute('''
    create table plan (
       id                 serial not null,
       road_number        varchar(16) not null,
       transfer_date      date not null,
       driver_id          integer references drivers (id) 
    );''')


@timing
def fill_tables(number):
    create_tables()

    str_drivers = '''
    insert into drivers (first_name, second_name, middle_name, employment_date) 
    values (%s, %s, %s, %s);'''

    str_plan = '''
        insert into plan (road_number, transfer_date, driver_id) 
        values (%s, %s, %s);'''

    for i in range(number):
        cur.execute(str_drivers, (random.choice(first_name_list), random.choice(second_name_list),
                    random.choice(middle_name_list), random.choice(employment_date_list)))
        cur.execute(str_plan, (random.choice(road_number_list), random.choice(transfer_date_list),
                               random.randint(1, i + 1)))


@timing
def inquiry_with_join():
    cur.execute('''select 
                        plan.id, 
                        plan.road_number, 
                        plan.transfer_date, 
                        drivers.first_name, 
                        drivers.second_name
                    from plan
                    inner join drivers
                    on plan.driver_id = drivers.id;''')


@timing
def inquiry_with_where(number):
    cur.execute('''select * from drivers where id = %s;''', (str(math.floor(number/3)), ))


def create_index():
    try:
        cur.execute('''create index plan_driver_id on plan (driver_id);''')
        cur.execute('''create index drivers_id on drivers (id);''')
    except:
        print("Індекси вже створені.")


def delete_index():
    try:
        cur.execute('''drop index plan_driver_id, drivers_id;''')
    except:
        print("Індекси вже видалені.")


def main():
    eternal_cycle = True
    while eternal_cycle:
        print("1 - Очистити таблиці.\n"
              "2 - Ввести кількість рядків.\n"
              "3 - Наповнити таблиці.\n"
              "4 - Виконати запити без індексів.\n"
              "5 - Створити індекси та виконати з ними запити.\n"
              "6 - Вихід.\n")
        choice = int(input("Ваш вибір: "))
        if choice == 1:
            drop_tables()
            eternal_cycle = True
        elif choice == 2:
            global num
            num = int(input("Введіть кількість рядків: "))
            eternal_cycle = True
        elif choice == 3:
            fill_tables(num)
            eternal_cycle = True
        elif choice == 4:
            inquiry_with_join()
            inquiry_with_where(num)
            eternal_cycle = True
        elif choice == 5:
            create_index()
            inquiry_with_join()
            inquiry_with_where(num)
            delete_index()
            eternal_cycle = True
        elif choice == 6:
            eternal_cycle = False
        else:
            print("Ви ввели неправильний номер, спробуте знову.\n")
            eternal_cycle = True


main()

