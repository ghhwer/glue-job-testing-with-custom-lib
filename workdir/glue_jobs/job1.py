import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from custom_lib import get_spark, read, write

def main():
    # init Glue context (and Spark context)
    spark_context = SparkContext()
    glue_context = GlueContext(spark_context)
    
    # init Glue job
    args = getResolvedOptions(
        sys.argv, 
        ['JOB_NAME', 'in_s3_file', 'out_s3_file']
    )
    job = Job(glue_context)
    job.init(args["JOB_NAME"], args)

    # Using my custom lib...
    spark = get_spark(glue_context)
    df = read(spark, args['in_s3_file'])
    write(df, args['out_s3_file'])
    
    job.commit()

if __name__ == '__main__':
    main()
