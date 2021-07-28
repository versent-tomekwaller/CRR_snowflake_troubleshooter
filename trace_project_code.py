import re
import sys
import os
import time
import json
import datetime
import logging
import traceback
from contextlib import contextmanager
from functools import lru_cache
import boto3
import botocore
import snowflake.connector
from dotenv import load_dotenv

LOGGING_LEVEL = logging.INFO
logger = logging.getLogger(os.path.basename(__file__))
logger.setLevel(LOGGING_LEVEL)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(LOGGING_LEVEL)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

load_dotenv()


# overwatch_landing_db.harvest.projects
# overwatch_landing_db.forecast.projects
# ->
# OVERWATCH_CURATED_DB.PROD.dim_project ->managers_t
#
# overwatch_landing_db.forecast.people
# overwatch_landing_db.forecast.projects
# overwatch_landing_db.forecast.assignments
# ->
# OVERWATCH_CURATED_DB.PROD.dim_people_project_assignment ->managers_t
#
# overwatch_landing_db.harvest.users & overwatch_landing_db.forecast.people  & overwatch_landing_db.harvest.users ->
# OVERWATCH_CURATED_DB.PROD.dim_people
# OVERWATCH_LANDING_DB.other.weekday
# ->
# OVERWATCH_CURATED_DB.PROD.dim_people_calendar ->managers_t



SQL_SCRIPTS = {
    # DIM_PROJECT LOADS
    "TABLE_overwatch_landing_db.harvest.projects": "SELECT * FROM overwatch_landing_db.harvest.projects WHERE CODE = '{0}'",
    "TABLE_overwatch_landing_db.forecast.projects": "SELECT * FROM overwatch_landing_db.forecast.projects WHERE CODE = '{0}'",
    "TABLE_dim_project": "SELECT * FROM OVERWATCH_CURATED_DB.PROD.dim_project  WHERE FORECAST_PROJECT_CODE = '{0}' OR HARVEST_PROJECT_CODE = '{0}'",

    # OVERWATCH_CURATED_DB.PROD.dim_people_project_assignment LOADS
    # Harvest & Forecast IDs

    "TABLE_OVERWATCH_CURATED_DB.PROD.dim_people_project_assignment": "SELECT * FROM OVERWATCH_CURATED_DB.PROD.dim_people_project_assignment WHERE FORECAST_PROJECT_ID = '{0}'",

    # OVERWATCH_CURATED_DB.PROD.dim_people_calendar


    #managers_t
    "TABLE_managers_t": "SELECT * FROM managers_t WHERE PROJECT_CODE = '{0}'",

    #OVERWATCH_LANDING_DB.other.BUNDLED_PROJECTS


    #CR_TEMP

    #STG.CR_SNAPSHOT_NEW_DATA

    #TEMP_ADD_SOLD_OTHER_REGION_AND_TARGET
    
    
    
    
    
    # FINAL SNAPSHOT
    "TABLE_OVERWATCH_ANALYTICS_DB.PROD.REP_CONSOLIDATED_REVENUE_WEEKLY_SNAPSHOT": "SELECT * FROM OVERWATCH_ANALYTICS_DB.PROD.REP_CONSOLIDATED_REVENUE_WEEKLY_SNAPSHOT WHERE PROJECTCODE = '{0}'",



    # TEMP TABLE SCRIPTS
    "CREATE managers_t" : """CREATE OR REPLACE TEMPORARY TABLE managers_t AS (
        WITH manager_array AS (
            SELECT DISTINCT 
                    UPPER(project.FORECAST_PROJECT_CODE) AS PROJECT_CODE 
                    , FULL_NAME  AS EMPLOYEE_NAME
            FROM        OVERWATCH_CURATED_DB.PROD.dim_people_project_assignment    AS people_project_assignments
            INNER JOIN  OVERWATCH_CURATED_DB.PROD.dim_project                      AS project
            ON project.forecast_project_id = people_project_assignments.forecast_project_id
            INNER JOIN  OVERWATCH_CURATED_DB.PROD.dim_people_calendar              AS people_calendar
            ON people_calendar.forecast_people_id = people_project_assignments.forecast_people_id
                AND people_calendar.date >= people_project_assignments.assignment_start_date
                AND people_calendar.date <= people_project_assignments.assignment_end_date
            WHERE IS_PROJECT_MANAGER = TRUE 
        )
        SELECT  DISTINCT    PROJECT_CODE 
                            , ARRAY_TO_STRING(
                                array_agg(manager_array.EMPLOYEE_NAME ) 
                                WITHIN GROUP (ORDER BY EMPLOYEE_NAME) 
                                OVER ( PARTITION BY PROJECT_CODE  )
                            ,', ') AS project_manager
        FROM manager_array
    )"""


}





def execute(cursor, *args):
    try:
        return cursor.execute(*args)
    except Exception as e:
        raise Exception(f'Failed to execute SQL: {args}') from e

def execute_fetch(cursor, *args):
    try:
        return cursor.execute(*args).fetchall()
    except Exception as e:
        raise Exception(f'Failed to execute SQL: {args}') from e


def get_snowflake_cursor():
    # Make snowflake connection
    ctx = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USERNAME"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )

    return ctx.cursor(snowflake.connector.DictCursor)


def get_result_length(cur, sql):
    result = execute_fetch(cur, sql)
    return len(result)

def check_result_length(cur, name, sql, quit_if_false=False):
    if get_result_length(cur, sql):
        print(f"\u2713 {name}")
        return True
    else:
        print(f"NOT PRESENT IN {name}")
        if quit_if_false:
            print("Quitting.")
            exit
        return False

def trace_status_of_project_code(project_code):
    print(f"Running checks in trace_status_of_project_code \t{project_code}")
    project_code_upper = project_code.upper()
    # Get dim+project loads
    cur = get_snowflake_cursor()

    check_result_length(cur, "overwatch_landing_db.harvest.projects", SQL_SCRIPTS["TABLE_overwatch_landing_db.harvest.projects"].format(project_code))
    check_result_length(cur, "overwatch_landing_db.forecast.projects", SQL_SCRIPTS["TABLE_overwatch_landing_db.forecast.projects"].format(project_code))
    if not check_result_length(cur, "dim_project", SQL_SCRIPTS["TABLE_dim_project"].format(project_code)):
        print("Not present in dim_project. Quitting.")
        return

    result = execute_fetch(cur, SQL_SCRIPTS["TABLE_dim_project"].format(project_code))
    HARVEST_PROJECT_ID = result[0]['HARVEST_PROJECT_ID']
    FORECAST_PROJECT_ID = result[0]['FORECAST_PROJECT_ID']
    print(f"PROJECT CODE:\t\t\t{project_code}")
    print(f"HARVEST_PROJECT_ID:\t\t{HARVEST_PROJECT_ID}")
    print(f"FORECAST_PROJECT_ID:\t{FORECAST_PROJECT_ID}")

    check_result_length(cur, "OVERWATCH_CURATED_DB.PROD.dim_people_project_assignment", SQL_SCRIPTS["TABLE_OVERWATCH_CURATED_DB.PROD.dim_people_project_assignment"].format(FORECAST_PROJECT_ID))

    execute(cur, SQL_SCRIPTS["CREATE managers_t"])
    check_result_length(cur, "managers_t", SQL_SCRIPTS["TABLE_managers_t"].format(project_code_upper))


    check_result_length(cur, "OVERWATCH_ANALYTICS_DB.PROD.REP_CONSOLIDATED_REVENUE_WEEKLY_SNAPSHOT", SQL_SCRIPTS["TABLE_OVERWATCH_ANALYTICS_DB.PROD.REP_CONSOLIDATED_REVENUE_WEEKLY_SNAPSHOT"].format(project_code_upper))

    result = execute_fetch(cur, SQL_SCRIPTS["TABLE_OVERWATCH_ANALYTICS_DB.PROD.REP_CONSOLIDATED_REVENUE_WEEKLY_SNAPSHOT"].format(project_code_upper))




    print("Finished.")
    print("-"*50)
    print("")





def main():
    trace_status_of_project_code('CUR-002-AppDev-TM')
    trace_status_of_project_code('CUR-001-CLD-TM')
    trace_status_of_project_code('CUR-003-AppDev-TM')






if __name__ == '__main__':
    try:
        main()
    except Exception as ex:
        logger.fatal("RUN STATE: FAIL")
        logger.fatal("TRACEBACK:")
        logger.fatal(print(traceback.format_exc()))
