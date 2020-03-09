"""To do"""

from pyspark.sql.types import *

input_schema = StructType([
    StructField('created_at', StringType(), True),
    StructField('event_name', StringType(), True),
    StructField('id', StringType(), True),
    StructField('ip', StringType(), True),
    StructField('metadata', StructType([
        StructField('action', StringType(), True),
        StructField('address', StringType(), True),
        StructField('admin_id', LongType(), True),
        StructField('bot_id', LongType(), True),
        StructField('invited_user_email', StringType(), True),
        StructField('job_title', StringType(), True),
        StructField('meeting_id', StringType(), True),
        StructField('meeting_time', StringType(), True),
        StructField('message_id', LongType(), True),
        StructField('name', StringType(), True),
        StructField('operator_id', StringType(), True),
        StructField('path', StringType(), True)
    ]), True),
    StructField('user_email', StringType(), True)
])

op_schema = StructType([
    StructField('id', StringType(), True),
    StructField('event_name', StringType(), True),
    StructField('created_at', StringType(), True),
    StructField('ip', StringType(), True),
    StructField('user_email', StringType(), True),
    StructField('action', StringType(), True),
    StructField('address', StringType(), True),
    StructField('admin_id', LongType(), True),
    StructField('bot_id', LongType(), True),
    StructField('invited_user_email', StringType(), True),
    StructField('job_title', StringType(), True),
    StructField('meeting_id', StringType(), True),
    StructField('meeting_time', StringType(), True),
    StructField('message_id', LongType(), True),
    StructField('name', StringType(), True),
    StructField('operator_id', StringType(), True),
    StructField('path', StringType(), True)
])

