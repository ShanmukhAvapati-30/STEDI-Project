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

# Script generated for node customer source
customersource_node1779703929660 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="customersource_node1779703929660")

# Script generated for node accelerometer source
accelerometersource_node1779703911149 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometersource_node1779703911149")

# Script generated for node Join
Join_node1779703975704 = Join.apply(frame1=accelerometersource_node1779703911149, frame2=customersource_node1779703929660, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1779703975704")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1779703975704, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779702895599", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1779704024411 = glueContext.getSink(path="s3://stedi-project-shanmukh/accelerometer-trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1779704024411")
AmazonS3_node1779704024411.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AmazonS3_node1779704024411.setFormat("json")
AmazonS3_node1779704024411.writeFrame(Join_node1779703975704)
job.commit()