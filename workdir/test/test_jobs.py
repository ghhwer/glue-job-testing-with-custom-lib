import sys
from unittest.mock import patch
from unittest.mock import Mock

from pyspark.context import SparkContext
from pyspark.sql import SparkSession

m = Mock()
sys.modules['custom_lib'] = m

from glue_jobs.job1 import main as j1main
from glue_jobs.job2 import main as j2main

state_objects = {}

# Mocked Functions
def mock_get_spark(glue_context):
    # Mocking a hypothetical get_spark function
    print('MOCKED - GET SPARK')
    return glue_context.spark_session

def mock_read(spark, file_path):
    # Mocking a hypothetical read function
    print('MOCKED - READ SPARK')
    rfp = file_path.split('/')[-1]
    return spark.read.csv(f'test/files/{rfp}', header=True)
    
def mock_write(df, file_dest):
    # Mocking a hypothetical write function
    print('MOCKED - WRITE SPARK')
    state_objects[file_dest] = {'data_frame':df}
    return file_dest

def getSparkContext():
    # Mocking SparkContext to allow for multiple glue jobs to be tested at once
    print('MOCKED - SparkContext()')
    return SparkSession.builder.getOrCreate().sparkContext

# Testing Job 1
@patch(
    "glue_jobs.job1.getResolvedOptions", 
    return_value={
        'JOB_NAME':'mock_name', 'in_s3_file':'s3://my_bucket/my_in/toy_dataset.csv', 'out_s3_file':'s3://my_bucket/my_out/out.csv'
    }, 
    autospec=True
)
@patch("glue_jobs.job1.get_spark", new=mock_get_spark)
@patch("glue_jobs.job1.read", new=mock_read)
@patch("glue_jobs.job1.write", new=mock_write)
@patch("glue_jobs.job1.SparkContext", new=getSparkContext)
@patch("glue_jobs.job1.Job", return_value=Mock(), autospec=True)
def test_job1(mock_get_resolved_options, mock_job):
    # Run as if it was a main
    j1main()
    # Get "saved object"
    df = state_objects['s3://my_bucket/my_out/out.csv']['data_frame']
    # Get columns
    cols = df.columns
    # assert
    assert cols == ['Number', 'City', 'Gender', 'Age', 'Income', 'Illness']
    
# Testing Job 2
@patch(
    "glue_jobs.job2.getResolvedOptions", 
    return_value={
        'JOB_NAME':'mock_name', 'in_s3_file':'s3://my_bucket/my_in/toy_dataset.csv', 'out_s3_file':'s3://my_bucket/my_out/out.csv'
    }, 
    autospec=True
)
@patch("glue_jobs.job2.get_spark", new=mock_get_spark)
@patch("glue_jobs.job2.read", new=mock_read)
@patch("glue_jobs.job2.write", new=mock_write)
@patch("glue_jobs.job2.SparkContext", new=getSparkContext)
@patch("glue_jobs.job2.Job", return_value=Mock(), autospec=True)
def test_job2(mock_get_resolved_options, mock_job):
    # Run as if it was a main
    j2main()
    # Get "saved object"
    df = state_objects['s3://my_bucket/my_out/out.csv']['data_frame']
    # Get columns
    cols = df.columns
    # assert
    assert cols == ['Number', 'City', 'Gender', 'Age', 'Income', 'Illness', 'my_new_col']
    