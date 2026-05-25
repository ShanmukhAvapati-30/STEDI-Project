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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1779712319043 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1779712319043")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1779712427168 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_trainer_trusted_node1779712427168")

# Script generated for node Join
Join_node1779712500883 = Join.apply(frame1=step_trainer_trusted_node1779712427168, frame2=accelerometer_trusted_node1779712319043, keys1=["sensorreadingtime"], keys2=["z"], transformation_ctx="Join_node1779712500883")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=Join_node1779712500883, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779712244765", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1779712523418 = glueContext.getSink(path="s3://stedi-project-shanmukh/machine-learning-curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1779712523418")
AmazonS3_node1779712523418.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1779712523418.setFormat("glueparquet", compression="snappy")
AmazonS3_node1779712523418.writeFrame(Join_node1779712500883)
job.commit()