from validation import hive_connection


def stage_table_drop(con,cur):
    cur.execute('drop table if exists data_fact_stage_table')
    cur.execute('drop table if exists airports_dim_stage_table')
    cur.execute('drop table if exists carriers_dim_stage_table')
    cur.execute('drop table if exists plane_dim_stage_table')


def stage_table_creation():
    con, cur = hive_connection()
    stage_table_drop(con,cur)
    cur.execute("""
    create table if not exists data_fact_stage_table(
    Yr smallint,
    Month tinyint,
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
    partitioned by(year smallint)
    row format delimited
        fields terminated by ','
        lines terminated by '\n'
    stored as TEXTFILE
        TBLPROPERTIES('skip.header.line.count'='1')
    """)
    cur.execute("""
    create table if not exists airports_dim_stage_table(
    IATA string,
    name string,
    city string,
    state string,
    country string,
    lat float,
    long float)
    row format delimited
        fields terminated by ','
        lines terminated by '\n'
    stored as TEXTFILE
       TBLPROPERTIES('skip.header.line.count'='1')     
    """)
    cur.execute("""
    create table if not exists carriers_dim_stage_table(
    code string,
    name string)
    row format delimited
        fields terminated by ','
        lines terminated by '\n'
    stored as TEXTFILE
       TBLPROPERTIES('skip.header.line.count'='1') 
    """)
    cur.execute("""
    create table if not exists plane_dim_stage_table(
    tailno string,
    type string,
    manufacturer string,
    issue_date string,
    model string,
    status string,
    aircraft_type string,
    engine_type string,
    year smallint)
    row format delimited
        fields terminated by ','
        lines terminated by '\n'
    stored as TEXTFILE
       TBLPROPERTIES('skip.header.line.count'='1')
    """)
    con.close()


def stage_table_load():
    con,cur=hive_connection()
    cur.execute("load data local inpath '/input_data/airports.csv' overwrite into table airports_dim_stage_table")
    cur.execute("load data local inpath '/input_data/carriers.csv' overwrite into table carriers_dim_stage_table")
    cur.execute("load data local inpath '/input_data/plane-data.csv' overwrite into table plane_dim_stage_table")
    for i in range(1987,2009):
        cur.execute("load data local inpath '/input_data/{}.csv.bz2'overwrite into table data_fact_stage_table partition(year={})".format(i,i))
    con.close()


