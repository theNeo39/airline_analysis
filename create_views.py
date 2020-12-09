from validation import hive_connection


def create_views():
    con,cur=hive_connection()
    cur.execute("""
    create view nos_arr_delay as
    select year, month,dayofmonth,
    case dayofweek
    when 1 then 'Monday'
    when 2 then 'Tuesday'
    when 3 then 'Wednesday'
    when 4 then 'Thursday'
    when 5 then 'Friday'
    when 6 then 'Saturday'
    when 7 then 'Sunday'
    end dayofweek,
    count(*) as flights_delay
    from data_fact_table
    where
    arrdelay>15
    group by year,month,dayofmonth,dayofweek
    """)

    cur.execute("""
    create view nos_dept_delay as
    select year, month,dayofmonth,
    case dayofweek
    when 1 then 'Monday'
    when 2 then 'Tuesday'
    when 3 then 'Wednesday'
    when 4 then 'Thursday'
    when 5 then 'Friday'
    when 6 then 'Saturday'
    when 7 then 'Sunday'
    end dayofweek,
    count(*) as flights_delay
    from data_fact_table
    where
    depdelay>15
    group by year,month,dayofmonth,dayofweek
    """)

    cur.execute("""
    create view timeinterval_dep_delay as
    select year,dept_interval,count(*) as flights_delay
    from
    ( select year,depdelay,
    case 
    when length(cast(deptime as string))<=2 then '0-1'
    when length(cast(deptime as string))=3 then substr(deptime,1,1)||'-'||cast(substr(deptime,1,1) as int)+1
    when length(cast(deptime as string))=4 and deptime>=1000 and deptime<=2359 then substr(deptime,1,2)||'-'||cast(substr(deptime,1,2) as int)+1
    when length(cast(deptime as string))=4 and deptime>=2500 then substr(deptime,1,2)||'-'||cast(substr(deptime,1,2) as int)+1
    when length(cast(deptime as string))=4 and deptime>=2400 and deptime<=2459 then '0-1' 
    end dept_interval
    from data_fact_table
    where depdelay>15 ) as time_interval
    group by year,dept_interval
    having dept_interval<>'25-26' and
    dept_interval<>'26-27' and
    dept_interval<>'27-28' and
    dept_interval<>'28-29' and
    dept_interval<>'29-30'
    order by flights_delay
    """)

    cur.execute("""
    create view timeinterval_arr_delay as
    select year,arr_interval,count(*) as flights_delay 
    from
    (select year,arrdelay,
    case 
    when length(cast(arrtime as string))<=2 then '0-1'
    when length(cast(arrtime as string))=3 then substr(arrtime,1,1)||'-'||cast(substr(arrtime,1,1) as int)+1
    when length(cast(arrtime as string))=4 and arrtime>=1000 and arrtime<=2359 then substr(arrtime,1,2)||'-'||cast(substr(arrtime,1,2) as int)+1
    when length(cast(arrtime as string))=4 and arrtime>=2400 and arrtime<=2459 then '0-1'
    when length(cast(arrtime as string))=4 and arrtime>=2500 then substr(arrtime,1,2)||'-'||cast(substr(arrtime,1,2) as int)+1
    end arr_interval
    from data_fact_table
    where arrdelay>15) as time_interval
    group by year,arr_interval
    having arr_interval<>'25-26' and
    arr_interval<>'26-27' and
    arr_interval<>'27-28' and
    arr_interval<>'28-29' and
    arr_interval<>'29-30'
    order by flights_delay
    """)

    cur.execute("""
    create view airport_depdelay as
    select year,origin,name, count(*) as flights_delay
    from 
    (select year,origin,depdelay,iata,name,city,state,country,lat,long
    from data_fact_table f
    left join airports_dim_table a on f.origin=a.iata) as airport_delay
    where depdelay>15
    group by year,origin,name
    order by flights_delay desc
    """)

    cur.execute("""
    create view airport_arrdelay as
    select year,dest,name, count(*) as flights_delay
    from
    (select year,dest,arrdelay,iata,name,city,state,country,lat,long
    from data_fact_table f
    left join airports_dim_table a on f.dest=a.iata) as airport_delay
    where arrdelay>15
    group by year,dest,name
    order by flights_delay desc
    """)

    cur.execute("""
    create view carrier_depdelay as
    select year,uniquecarrier,name,count(*) as flights_delay
    from 
    (select year,uniquecarrier, depdelay,name
    from data_fact_table f
    left join carriers_dim_table c on f.uniquecarrier=c.code) as carrier_delay 
    where depdelay>15
    group by year,uniquecarrier,name
    order by flights_delay
    """)

    cur.execute("""
    create view oldplane_depdelay as
    select year,count(*) as flights_delay
    from
    (select depdelay,p.year
    from data_fact_table f
    left join plane_dim_table p on f.tailnum=p.tailno) as oldplane_delay
    where depdelay>15
    group by year
    order by flights_delay
    """)