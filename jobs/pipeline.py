"""
pipeline.py
~~~~~~~~~~
This is the codebase for ETL process for running the pipeline
It can be executed as python code or submitted using spark-submit command

"""
from argparse import ArgumentParser
from utils.utilities import start_spark, read_config
from utils.schema import input_schema

__author__ = 'Soyel Alam'
__email__ = 'soyel.alam@ucdconnect.ie'


# file_path = 's3a://dataeng-challenge/8uht6u8bh/events/*/*/*/*'
def extract(spark, logger, file_path):
    """Load data from json file and return a DataFrame

    :param spark: Spark session object
    :param logger: Spark logger object
    :param file_path: path of the json file
    :return: Spark DataFrame"""

    try:
        df = spark.read.json(file_path, input_schema)
    except Exception as err:
        logger.warn("Exception in the extract")
        logger.exception(err)
        raise err

    return df


def transform(spark, logger, input_df):
    """Transform an input spark DataFrame into an output DataFrame

    Based on the transformation logic
    :param spark: Spark session object
    :param logger: Py4j logger object
    :param input_df: Input DataFrame
    :return : Transformed DataFrame"""

    # todo: remove the query and place in config file
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
                        from event_frame
                        where id is not null
                        ) where rn = 1"""

    try:
        input_df.createOrReplaceTempView('event_frame')
        # applying transformation
        op_df = spark.sql(sql)
        op_df.printSchema()
        op_df.show(truncate=False)
    except Exception as err:
        logger.warn("Exception in the transform")
        logger.exception(err)
        raise err

    return op_df


def load(df, out_path):
    """Load data from json file and return a DataFrame
    Please remove the commented code for persisting the data into s3 bucket

        :param df: output DataFrame
        :param out_path: output file path
        :return: True for success"""

    if out_path:
        df.write.parquet(out_path, mode='overwrite')

    return True


def run(config_file, inc_date, env):
    """This method would be exposed to orchestration code.
    It would be responsible for performing ETL and cleanup.
    It reads the configuration file and run the pipeline based on the
    config provided

    :param config_file: path of the config file
    :param inc_date: Incremental date to load default is full load
    :param env: Environment i.e. dev/prod default is dev
    :return: True for success"""

    spark, logger = start_spark(app_name='event_frame', master='local')
    logger.warn('Starting Pipeline')
    try:
        # todo: refactor and taking out the config parser to a separate submitter module
        # reading the config file from the config path
        config = read_config(config_file)
        if config[env]['input_file_path']:
            df = extract(spark, logger, file_path='{}{}'.format(config[env]['input_file_path'], inc_date))
            op_df = transform(spark, logger, df)
        if config[env]['op_file_path']:
            load(op_df, config[env]['op_file_path'])
        logger.warn('Process Completed Successfully')
    except Exception as err:
        logger.warn("Exception in the pipeline")
        logger.exception(err)
        raise err


if __name__ == '__main__':
    """Read the job arguments and execute the pipelines"""
    parser = ArgumentParser(description='Run a PySpark job')
    parser.add_argument('--conf-file', type=str, dest='conf_file',
                        help="path of config.ini to be read Usage: config/config.ini",
                        default='config/config.ini')
    parser.add_argument('--env', type=str, dest='env',
                        help="run environment Usage: dev/prod", required=True, choices=('dev', 'prod'))
    parser.add_argument('--inc-date', type=str, dest='inc_date',
                        help="Incremental loading date Usage: YYYY/MM/DD",
                        default='*/*/*')

    args = parser.parse_args()
    run(args.conf_file, args.inc_date, args.env)
