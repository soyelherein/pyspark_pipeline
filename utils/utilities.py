import configparser

from pyspark.sql import SparkSession


def start_spark(app_name='intercom', master='local'):
    spark = SparkSession.builder.master(master).appName(app_name).getOrCreate()
    return spark


def read_config(config_file):
    config = configparser.ConfigParser()
    config.read(config_file)
    return config


if __name__ == '__main':
    pass
