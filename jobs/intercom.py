import argparse
import sys

from utils.utilities import start_spark, read_config
from utils.schema import input_schema
import re
from pyspark.sql.types import *
from pyspark.sql import DataFrame

__author__ = 'soyel alam'
__email__ = 'soyel.alam@ucdconnect.ie'


# file_path = 's3a://dataeng-challenge/8uht6u8bh/events/*/*/*/*'
def extract(spark, file_path):
    """Load data from json file and return a DataFrame

    :param spark: Spark session object
    :param file_path: path of the json file
    :return: Spark DataFrame"""

    return spark.read.json(file_path, input_schema)


def transform(spark, input_df) -> DataFrame:
    """Transform an input spark DataFrame into an output DataFrame

    Based on the transformation logic
    :param spark: Spark session object
    :param input_df: Input DataFrame
    :return : Transformed DataFrame"""

    input_df.createOrReplaceTempView('intercom')
    sql = """select id
                        ,event_name
                        ,created_at
                        ,ip
                        ,user_email
                        ,metadata.action as action
                        ,regexp_replace(metadata.address,'\n',' ') as address
                        ,metadata.admin_id as admin_id
                        ,metadata.bot_id as bot_id
                        ,metadata.invited_user_email as invited_user_email
                        ,metadata.job_title as job_title
                        ,metadata.meeting_id as meeting_id
                        ,metadata.meeting_time as meeting_time
                        ,metadata.message_id as message_id
                        ,metadata.name as name
                        ,metadata.operator_id as operator_id
                        ,metadata.path as path
                from(
                 select id
                        ,event_name
                        ,created_at
                        ,ip
                        ,user_email
                        ,metadata
                        ,row_number() over(partition by id,event_name order by created_at desc) rn
                        from intercom
                        where id is not null
                        ) where rn = 1"""

    op_df = spark.sql(sql)
    op_df.printSchema()
    op_df.show(truncate=False)
    return op_df


def load(df, out_path):
    """Load data from json file and return a DataFrame

        :param df: output DataFrame
        :param out_path: output file path
        :return: True for success"""

    df.write.parquet(out_path, mode='overwrite')

    return True


def run(input_file_path='tests/input/intercom_test_data.json', op_file_path='tests/output/'):
    """This method would be exposed to orchestration code.
    It would be responsible for performing ETL and cleanup
    :param input_file_path: path of the input data
    :param op_file_path: path of the output data
    :return: True for success"""

    spark = start_spark()
    df = extract(spark, input_file_path)
    op_df = transform(spark, df)
    load(op_df, op_file_path)
    spark.stop()

    return True


if __name__ == '__main__':
    """Read the job arguments and execute the pipeline"""
    parser = argparse.ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--conf-file', type=str, dest='conf_file',
                        help="path of config.ini to be read Usage: config/config.ini",
                        default='config/config.ini')
    parser.add_argument('--env', type=str, dest='env',
                        help="run environment Usage: dev/prod", required=True, choices=('dev', 'prod'))
    parser.add_argument('--inc-date', type=str, dest='inc_date',
                        help="Incremental loading date Usage: YYYY/MM/DD")

    try:
        args = parser.parse_args()
        config = read_config(args.conf_file)
        inc_date = args.inc_date if args.inc_date else '*/*/*'
        config = read_config(args.conf_file)

        if config[args.env]['input_file_path'] and config[args.env]['op_file_path']:
            run(config[args.env]['input_file_path']+inc_date, config[args.env]['op_file_path'])

        print("Process completed")

    except Exception as e:
        print("Exception in pipeline execution")
        print(e)
        sys.exc_info()
        sys.exit(1)
