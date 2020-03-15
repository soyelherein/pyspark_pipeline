"""
utilities.py
~~~~~~~~
Module containing helper functions
"""
import configparser
from pyspark.sql import SparkSession

__author__ = 'Soyel Alam'


def start_spark(app_name='sample_spark', master='local'):
    """Create spark session and py4j logger object

    :param app_name: Application Name
    :param master: spark cluster address default local
    """
    # todo: Programmatically loading aws jars and custom jars
    spark = SparkSession.builder.master(master).appName(app_name).getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    _log4j = spark._jvm.org.apache.log4j
    logger = _log4j.LogManager.getLogger(app_name)
    return spark, logger


def read_config(config_file):
    """Read .ini configuration file and return the parsed disctionary

    :param config_file: path to the config file i.e. config/config.ini
    """
    config = configparser.ConfigParser()
    config.read(config_file)
    return config


if __name__ == '__main':
    pass
