import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer source
accelerometersource_node1779706771590 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometersource_node1779706771590")

# Script generated for node customer source
customersource_node1779706801171 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customersource_node1779706801171")

# Script generated for node Join
Join_node1779707548749 = Join.apply(frame1=accelerometersource_node1779706771590, frame2=customersource_node1779706801171, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1779707548749")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1779707548749, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779702895599", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1779707032195 = glueContext.getSink(path="s3://stedi-project-shanmukh/customer-curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1779707032195")
AmazonS3_node1779707032195.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
AmazonS3_node1779707032195.setFormat("glueparquet", compression="snappy")
AmazonS3_node1779707032195.writeFrame(Join_node1779707548749)
job.commit()