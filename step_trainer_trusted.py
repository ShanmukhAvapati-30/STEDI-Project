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

# Script generated for node step_trainer_landing
step_trainer_landing_node1779709988171 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1779709988171")

# Script generated for node customer_curated
customer_curated_node1779709839493 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="customer_curated_node1779709839493")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1779710839485 = ApplyMapping.apply(frame=step_trainer_landing_node1779709988171, mappings=[("sensorreadingtime", "long", "right_sensorreadingtime", "long"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "right_distancefromobject", "int")], transformation_ctx="RenamedkeysforJoin_node1779710839485")

# Script generated for node Join
Join_node1779710433499 = Join.apply(frame1=customer_curated_node1779709839493, frame2=RenamedkeysforJoin_node1779710839485, keys1=["email"], keys2=["right_sensorreadingtime"], transformation_ctx="Join_node1779710433499")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1779710433499, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779702895599", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1779710207471 = glueContext.getSink(path="s3://stedi-project-shanmukh/step-trainer-trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1779710207471")
AmazonS3_node1779710207471.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
AmazonS3_node1779710207471.setFormat("glueparquet", compression="snappy")
AmazonS3_node1779710207471.writeFrame(Join_node1779710433499)
job.commit()