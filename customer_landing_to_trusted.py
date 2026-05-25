import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1779695780087 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1779695780087")

# Script generated for node SQL Query
SqlQuery768 = '''
SELECT *
FROM myDataSource
WHERE shareWithResearchAsOfDate IS NOT NULL
'''
SQLQuery_node1779695829733 = sparkSqlQuery(glueContext, query = SqlQuery768, mapping = {"myDataSource":AWSGlueDataCatalog_node1779695780087}, transformation_ctx = "SQLQuery_node1779695829733")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1779695829733, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1779695532920", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1779695895268 = glueContext.getSink(path="s3://stedi-project-shanmukh/customer-trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1779695895268")
AmazonS3_node1779695895268.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
AmazonS3_node1779695895268.setFormat("json")
AmazonS3_node1779695895268.writeFrame(SQLQuery_node1779695829733)
job.commit()