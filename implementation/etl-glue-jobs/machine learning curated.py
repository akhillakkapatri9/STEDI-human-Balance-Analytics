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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1783403095361 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-lake-housey/step_trainer/trusted/"], "recurse": True}, transformation_ctx="step_trainer_trusted_node1783403095361")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1783402925348 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-lake-housey/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1783402925348")

# Script generated for node SQL Query
SqlQuery3069 = '''
select a.x,a.y,a.z,s.*
from s
join a
on a.timestamp=s.sensorreadingtime
'''
SQLQuery_node1783403144762 = sparkSqlQuery(glueContext, query = SqlQuery3069, mapping = {"a":accelerometer_trusted_node1783402925348, "s":step_trainer_trusted_node1783403095361}, transformation_ctx = "SQLQuery_node1783403144762")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783403144762, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783403490183", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1783403502592 = glueContext.getSink(path="s3://stedi-data-lake-housey/machine learning/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1783403502592")
AmazonS3_node1783403502592.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
AmazonS3_node1783403502592.setFormat("json")
AmazonS3_node1783403502592.writeFrame(SQLQuery_node1783403144762)
job.commit()