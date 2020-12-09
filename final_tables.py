from validation import hive_connection


def optimized_table_drop(cur):
    cur.execute('drop table if exists airports_dim_table')
    cur.execute('drop table if exists carriers_dim_table')
    cur.execute('drop table if exists plane_dim_table')
    cur.execute('drop table if exists data_fact_table')


def optimized_table_creation_load():
    con,cur=hive_connection()
    optimized_table_drop(cur)
    cur.execute("""
    create table airports_dim_table 
    AS
    select regexp_replace(iata,'"','') as iata,regexp_replace(name,'"','') as name,regexp_replace(city,'"','') as city,regexp_replace(state,'"','') as state,regexp_replace(country,'"','')as country,lat,long
    from airports_dim_stage_table;
    """)
    cur.execute("""
    create table carriers_dim_table
    AS
    select regexp_replace(code, '"','') as code,regexp_replace(name,'"','') as name
    from carriers_dim_stage_table;
    """)
    cur.execute("""
    create table if not exists plane_dim_table
    AS
    select * from plane_dim_stage_table
    """)
    cur.execute("""
    create table if not exists data_fact_table(
    DayofMonth tinyint,
    DayOfWeek tinyint,
    DepTime	smallint,
    CRSDepTime smallint,	
    ArrTime smallint,
    CRSArrTime smallint,
    UniqueCarrier string,
    FlightNum int,
    TailNum string,	
    ActualElapsedTime smallint,
    CRSElapsedTime smallint,
    AirTime smallint,
    ArrDelay smallint,	
    DepDelay smallint,
    Origin	string,
    Dest string,
    Distance int,
    TaxiIn	smallint,
    TaxiOut smallint,
    Cancelled tinyint,
    CancellationCode string,	
    Diverted tinyint,
    CarrierDelay smallint,
    WeatherDelay smallint,
    NASDelay smallint,
    SecurityDelay smallint,	
    LateAircraftDelay smallint)
    partitioned by(year smallint,month tinyint)
    """)
    for i in range(1987,2009):
        cur.execute("""
        insert overwrite table data_fact_table
        partition(year,month)
        select
        DayofMonth,
        DayOfWeek,
        DepTime,
        CRSDepTime,
        ArrTime,
        CRSArrTime,
        UniqueCarrier,
        FlightNum,
        TailNum,
        ActualElapsedTime,
        CRSElapsedTime,
        AirTime,
        ArrDelay,
        DepDelay,
        Origin,
        Dest,
        Distance,
        TaxiIn,
        TaxiOut,
        Cancelled,
        CancellationCode,
        Diverted,
        CarrierDelay,
        WeatherDelay,
        NASDelay,
        SecurityDelay,
        LateAircraftDelay,
        year,
        month
        from data_fact_stage_table
        where year={}
        """.format(i))
        print(i)
    con.close()