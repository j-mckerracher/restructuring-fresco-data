import time
import datetime
import pytz
import psycopg2 as psycopg2

conn = psycopg2.connect(host="localhost", dbname="postgres", user="postgres", password="postgres")
cur = conn.cursor()


def print_mountain_time():
    # Get the current UTC time
    utc_now = datetime.datetime.now(pytz.utc)

    # Convert the UTC time to Mountain Time
    mountain_time = utc_now.astimezone(pytz.timezone('US/Mountain'))

    # Format the mountain_time to only include up to seconds
    formatted_time = mountain_time.strftime('%H:%M:%S')

    return formatted_time


def drop_tables_if_exist():
    """Drops multiple tables if they exist."""
    print(f"Starting drop_tables_if_exist")

    table_names = [
        "event_cpuuser",
        "event_gpu_usage",
        "event_nfs",
        "event_memused_minus_diskcache",
        "event_block",
        "event_memused",
        "temp_host_data",
        "temp_job_data",
        "temp_job_data_single_node",
        "merged_data",
        "block_and_cpu",
        "block_cpu_gpu",
        "block_cpu_gpu_memused",
        "block_cpu_gpu_memused_nodisk",
        "full_merged"
    ]
    tables = ", ".join(table_names)
    drop_tables_query = f'DROP TABLE IF EXISTS {tables};'
    try:
        cur.execute(drop_tables_query)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"Error dropping tables: {e}")


def print_table_cols(table_name):
    """Prints the columns of a table."""
    get_columns_query = "SELECT column_name FROM information_schema.columns WHERE table_name = %s;"
    try:
        cur.execute(get_columns_query, (table_name,))
        column_names = [row[0] for row in cur.fetchall()]
        print(f"Columns in {table_name}: {column_names}")
    except Exception as e:
        print(f"Error fetching columns for table {table_name}: {e}")


def create_temp_host_data_table(month, year) -> bool:
    print(f"Starting create_temp_host_data_table at {print_mountain_time()}")
    dedup_host_data = f"""
    CREATE TEMP TABLE temp_host_data AS 
    SELECT DISTINCT 
        * 
    FROM 
        host_data 
    WHERE 
        EXTRACT(MONTH FROM host_data.time) = {month} 
    AND 
        EXTRACT(YEAR FROM host_data.time) = {year};
    """

    cur.execute(dedup_host_data)

    # Check the number of rows in temp_job_data
    cur.execute("SELECT COUNT(*) FROM temp_host_data;")
    row_count = cur.fetchone()[0]

    if row_count == 0:
        print(f"temp_host_data table has 0 rows for month: {month} year: {year}")
        return False

    return True


def create_temp_job_data_table(month, year) -> bool:
    print(f"Starting create_temp_job_data_table at {print_mountain_time()}")
    dedup_job_data = f"""
    CREATE TEMP TABLE temp_job_data AS 
    SELECT DISTINCT 
        * 
    FROM 
        job_data 
    WHERE 
        EXTRACT(MONTH FROM job_data.submit_time) = {month} 
    AND 
        EXTRACT(YEAR FROM job_data.submit_time) = {year};
    """

    cur.execute(dedup_job_data)

    # Check the number of rows in temp_job_data
    cur.execute("SELECT COUNT(*) FROM temp_job_data;")
    row_count = cur.fetchone()[0]

    if row_count == 0:
        print(f"temp_job_data table has 0 rows for month: {month} year: {year}")
        return False

    return True


def remove_multi_host_jobs_from_job_accounting_data():
    print(f"Starting remove_multi_host_jobs_from_job_accounting_data at {print_mountain_time()}")
    filter_job_data_for_single_nodes = """
    CREATE TEMP TABLE temp_job_data_single_node AS 
    SELECT 
        * 
    FROM 
        temp_job_data 
    WHERE 
        array_length(host_list, 1) = 1;
    """

    cur.execute(filter_job_data_for_single_nodes)
    print("remove_multi_host_jobs_from_job_accounting_data done")


def inner_join_on_job_id():
    print(f"Starting inner_join_on_job_id at {print_mountain_time()}")
    merge_tables_on_jid = """
        CREATE TEMP TABLE merged_data AS 
        SELECT
            h.time,
            h.host,
            h.jid,
            h.event,
            h.unit,
            h.value,
            j.exitcode,
            j.timelimit,
            j.ngpus,
            j.account,
            j.username,
            j.start_time,
            j.nhosts,
            j.end_time,
            j.host_list,
            j.queue,
            j.submit_time,
            j.ncores,
            j.jobname
        FROM temp_host_data h 
        JOIN temp_job_data_single_node j ON h.jid = j.jid;
        """
    cur.execute(merge_tables_on_jid)

    print("inner_join_on_job_id done")


def group_by_metric():
    print(f"Starting group_by_metric at {print_mountain_time()}")
    # Get the distinct events from merged_data
    get_events_query = "SELECT DISTINCT event FROM merged_data;"
    cur.execute(get_events_query)
    events = [row[0] for row in cur.fetchall()]
    print(f'events = {events}')

    # Loop through each event and create the temp table
    for event in events:
        table_name = f"event_{event}"

        # Create the temp table for the current event
        create_temp_table_query = f"CREATE TEMP TABLE {table_name} AS SELECT * FROM merged_data WHERE event = %s;"
        cur.execute(create_temp_table_query, (event,))
        print(f"{table_name} created")

    print("group_by_metric done")


def create_block_and_cpu_temp_table():
    print(f"Starting create_block_and_cpu_temp_table at {print_mountain_time()}")

    # Create indexes on the joining columns for the tables involved in the join
    create_idx_event_block = "CREATE INDEX IF NOT EXISTS idx_event_block ON event_block(time, jid, host);"
    cur.execute(create_idx_event_block)

    create_idx_event_cpuuser = "CREATE INDEX IF NOT EXISTS idx_event_cpuuser ON event_cpuuser(time, jid, host);"
    cur.execute(create_idx_event_cpuuser)

    conn.commit()  # Commit after creating indexes

    # Perform the join operation
    merge_block_and_cpu_user = """
    CREATE TEMP TABLE block_and_cpu AS 
    SELECT 
        b.time AS time_block,
        b.value AS value_block,
        b.ngpus AS ngpus_block,
        b.submit_time AS submit_time_block,
        b.start_time AS start_time_block,
        b.end_time AS end_time_block,
        b.timelimit AS timelimit_block,
        b.nhosts AS nhosts_block,
        b.ncores AS ncores_block,
        b.account AS account_block,
        b.queue AS queue_block,
        b.host AS host_block,
        b.jid AS jid_block,
        b.event AS event_block,
        b.unit AS unit_block,
        b.jobname AS jobname_block,
        b.exitcode AS exitcode_block,
        b.host_list AS host_list_block,
        b.username AS username_block,
        c.time AS time_cpuuser,
        c.value AS value_cpuuser,
        c.ngpus AS ngpus_cpuuser,
        c.submit_time AS submit_time_cpuuser,
        c.start_time AS start_time_cpuuser,
        c.end_time AS end_time_cpuuser,
        c.timelimit AS timelimit_cpuuser,
        c.nhosts AS nhosts_cpuuser,
        c.ncores AS ncores_cpuuser,
        c.account AS account_cpuuser,
        c.queue AS queue_cpuuser,
        c.host AS host_cpuuser,
        c.jid AS jid_cpuuser,
        c.event AS event_cpuuser,
        c.unit AS unit_cpuuser,
        c.jobname AS jobname_cpuuser,
        c.exitcode AS exitcode_cpuuser,
        c.host_list AS host_list_cpuuser,
        c.username AS username_cpuuser
    FROM 
        event_block b 
    JOIN 
        event_cpuuser c ON b.time = c.time AND b.jid = c.jid AND b.host = c.host 
    """

    cur.execute(merge_block_and_cpu_user)

    conn.commit()  # Commit after the join operation

    # Drop the indexes, since they were only needed for the join operation
    drop_idx_event_block = "DROP INDEX idx_event_block;"
    cur.execute(drop_idx_event_block)

    drop_idx_event_cpuuser = "DROP INDEX idx_event_cpuuser;"
    cur.execute(drop_idx_event_cpuuser)

    conn.commit()  # Commit after dropping indexes

    print("create_block_and_cpu_temp_table done")


def create_block_cpu_gpu_temp_table():
    print(f"Starting create_block_cpu_gpu_temp_table at {print_mountain_time()}")
    # Create indexes
    create_idx_block_and_cpu = "CREATE INDEX IF NOT EXISTS idx_block_and_cpu ON block_and_cpu(time_block, jid_block, host_block);"
    cur.execute(create_idx_block_and_cpu)

    create_idx_event_gpu_usage = "CREATE INDEX IF NOT EXISTS idx_event_gpu_usage ON event_gpu_usage(time, jid, host);"
    cur.execute(create_idx_event_gpu_usage)

    conn.commit()  # Commit after creating indexes

    # TODO REMOVE AFTER DEV
    print_table_cols("block_and_cpu")
    print_table_cols("event_gpu_usage")

    # Perform the join operation
    create_block_cpu_gpu = """
    CREATE TEMP TABLE block_cpu_gpu AS 
    SELECT 
        bc.*,
        g.value AS "value_gpu"
    FROM block_and_cpu bc 
    LEFT JOIN event_gpu_usage g 
    ON bc."time_block" = g.time AND bc."jid_block" = g.jid AND bc."host_block" = g.host;
    """
    cur.execute(create_block_cpu_gpu)

    conn.commit()  # Commit after the join operation

    # Drop the indexes
    drop_idx_block_and_cpu = "DROP INDEX idx_block_and_cpu;"
    cur.execute(drop_idx_block_and_cpu)

    drop_idx_event_gpu_usage = "DROP INDEX idx_event_gpu_usage;"
    cur.execute(drop_idx_event_gpu_usage)

    conn.commit()  # Commit after dropping indexes

    print("create_block_cpu_gpu_temp_table done")


def create_block_cpu_gpu_memused_temp_table():
    print(f"Starting create_block_cpu_gpu_memused_temp_table at {print_mountain_time()}")
    # Create indexes on the joining columns for both tables
    create_idx_block_cpu_gpu = "CREATE INDEX IF NOT EXISTS idx_block_cpu_gpu ON block_cpu_gpu(time, jid, host);"
    cur.execute(create_idx_block_cpu_gpu)

    create_idx_event_memused = "CREATE INDEX IF NOT EXISTS idx_event_memused ON event_memused(time, jid, host);"
    cur.execute(create_idx_event_memused)

    conn.commit()  # Commit after creating indexes

    # Create the new table by joining the two existing tables
    create_block_cpu_gpu_memused = """
    CREATE TEMP TABLE block_cpu_gpu_memused AS 
    SELECT 
        bcg.*, 
        m.value AS "value_memused"
    FROM 
        block_cpu_gpu bcg 
    JOIN 
        event_memused m ON bcg."time" = m.time AND bcg."jid" = m.jid AND bcg."host" = m.host;
    """
    cur.execute(create_block_cpu_gpu_memused)

    conn.commit()  # Commit after the join operation

    # Drop the indexes, since they were only needed for the join operation
    drop_idx_block_cpu_gpu = "DROP INDEX idx_block_cpu_gpu;"
    cur.execute(drop_idx_block_cpu_gpu)

    drop_idx_event_memused = "DROP INDEX idx_event_memused;"
    cur.execute(drop_idx_event_memused)

    conn.commit()  # Commit after dropping indexes

    print("create_block_cpu_gpu_memused_temp_table done")


def create_block_cpu_gpu_memused_nodisk_temp_table():
    print(f"Starting create_block_cpu_gpu_memused_nodisk_temp_table at {print_mountain_time()}")
    # Create indexes on the joining columns for both tables
    create_idx_block_cpu_gpu_memused = "CREATE INDEX IF NOT EXISTS idx_block_cpu_gpu_memused ON block_cpu_gpu_memused(time, jid, host);"
    cur.execute(create_idx_block_cpu_gpu_memused)

    create_idx_event_memused_minus_diskcache = "CREATE INDEX IF NOT EXISTS idx_event_memused_minus_diskcache ON event_memused_minus_diskcache(time, jid, host);"
    cur.execute(create_idx_event_memused_minus_diskcache)

    conn.commit()  # Commit after creating indexes

    # Create the new table by joining the two existing tables
    create_block_cpu_gpu_memused_nodisk = """
    CREATE TEMP TABLE block_cpu_gpu_memused_nodisk AS 
    SELECT 
        bcgm.*, 
        md.value AS "value_memused_minus_diskcache"
    FROM 
        block_cpu_gpu_memused bcgm 
    JOIN 
        event_memused_minus_diskcache md ON bcgm."time" = md.time AND bcgm."jid" = md.jid AND bcgm."host" = md.host;
    """
    cur.execute(create_block_cpu_gpu_memused_nodisk)

    conn.commit()  # Commit after the join operation

    # Drop the indexes, since they were only needed for the join operation
    drop_idx_block_cpu_gpu_memused = "DROP INDEX idx_block_cpu_gpu_memused;"
    cur.execute(drop_idx_block_cpu_gpu_memused)

    drop_idx_event_memused_minus_diskcache = "DROP INDEX idx_event_memused_minus_diskcache;"
    cur.execute(drop_idx_event_memused_minus_diskcache)

    conn.commit()  # Commit after dropping indexes

    print("create_block_cpu_gpu_memused_nodisk_temp_table done")


def create_full_merged_temp_table():
    print(f"Starting create_full_merged_temp_table at {print_mountain_time()}")
    # Create indexes on the joining columns for both tables
    create_idx_block_cpu_gpu_memused_nodisk = "CREATE INDEX IF NOT EXISTS idx_block_cpu_gpu_memused_nodisk ON block_cpu_gpu_memused_nodisk(time, jid, host);"
    cur.execute(create_idx_block_cpu_gpu_memused_nodisk)

    create_idx_event_nfs = "CREATE INDEX IF NOT EXISTS idx_event_nfs ON event_nfs(time, jid, host);"
    cur.execute(create_idx_event_nfs)

    conn.commit()  # Commit after creating indexes

    # Create the new table by joining the two existing tables
    create_full_merged = """
    CREATE TEMP TABLE full_merged AS 
    SELECT 
        bcgmn.*, 
        n.value AS "nfs"
    FROM 
        block_cpu_gpu_memused_nodisk bcgmn 
    JOIN 
        event_nfs n ON bcgmn."time" = n.time AND bcgmn."jid" = n.jid AND bcgmn."host" = n.host;
    """
    cur.execute(create_full_merged)

    conn.commit()  # Commit after the join operation

    # Drop the indexes, since they were only needed for the join operation
    drop_idx_block_cpu_gpu_memused_nodisk = "DROP INDEX idx_block_cpu_gpu_memused_nodisk;"
    cur.execute(drop_idx_block_cpu_gpu_memused_nodisk)

    drop_idx_event_nfs = "DROP INDEX idx_event_nfs;"
    cur.execute(drop_idx_event_nfs)

    conn.commit()  # Commit after dropping indexes

    print("create_full_merged_temp_table done")


def export_full_merged_data_to_csv(save_destination):
    print(f"Starting export_full_merged_data_to_csv at {print_mountain_time()}")
    copy_query = f"COPY full_merged TO STDOUT WITH CSV HEADER"
    with open(save_destination, 'w', newline='') as csvfile:
        cur.copy_expert(copy_query, csvfile)

    print(f"Data saved to {save_destination}")


def merge_and_export_data(mm: int, yyyy: int):
    mm = 9  # TODO REMOVE AFTER TESTING
    yyyy = 2022  # TODO REMOVE AFTER TESTING

    # 0. Read Data & Remove Duplicates -> exit if either temp table is empty
    if not create_temp_host_data_table(mm, yyyy) or not create_temp_job_data_table(mm, yyyy):
        return

    # 1. remove multi-host jobs
    remove_multi_host_jobs_from_job_accounting_data()

    # 2. inner join on job id
    inner_join_on_job_id()

    # 3. group dfs by metric
    group_by_metric()

    # 4. create temp tables
    create_block_and_cpu_temp_table()
    create_block_cpu_gpu_temp_table()
    create_block_cpu_gpu_memused_temp_table()
    create_block_cpu_gpu_memused_nodisk_temp_table()
    create_full_merged_temp_table()

    # 5. export to csv
    export_full_merged_data_to_csv(f"{mm}_{yyyy}_full_merged.csv")


def merge_and_export_all_data():
    # drop all temporary tables
    drop_tables_if_exist()

    months_years = {}  # {month: year}

    # SQL query to get distinct month-year combinations
    months_years_query = """
        SELECT 
        EXTRACT(MONTH FROM time) AS month,
        EXTRACT(YEAR FROM time) AS year
    FROM 
        host_data
    GROUP BY 
        EXTRACT(MONTH FROM time), 
        EXTRACT(YEAR FROM time)
    ORDER BY 
        year, 
        month;
    """

    # Execute the query
    cur.execute(months_years_query)
    results = cur.fetchall()

    # Populate the months_years dictionary from the results
    for row in results:
        month = int(row[0])  # Convert the float month to an integer
        year = int(row[1])  # Convert the float year to an integer
        months_years[month] = year

    # Iterate through the dictionary and call the merge_and_export_data function
    for month, year in months_years.items():
        merge_and_export_data(month, year)
        break  # TODO REMOVE AFTER TESTING


if __name__ == "__main__":
    print(f"Started at {print_mountain_time()}")
    start_time = time.time()

    merge_and_export_all_data()

    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Merging and exporting all data took {elapsed_time:.2f} seconds to run.")
