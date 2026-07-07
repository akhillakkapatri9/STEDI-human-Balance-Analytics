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

# Script generated for node customer_curated
customer_curated_node1783402544272 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-lake-housey/customer/curated/"], "recurse": True}, transformation_ctx="customer_curated_node1783402544272")

# Script generated for node step_trainer_landing
step_trainer_landing_node1783402504041 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-lake-housey/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_landing_node1783402504041")

# Script generated for node SQL Query
SqlQuery3130 = '''
select s.*
from s 
inner join c
on s.serialnumber=c.serialnumber
'''
SQLQuery_node1783402592909 = sparkSqlQuery(glueContext, query = SqlQuery3130, mapping = {"s":step_trainer_landing_node1783402504041, "c":customer_curated_node1783402544272}, transformation_ctx = "SQLQuery_node1783402592909")

# Script generated for node step_trainer_trusted
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783402592909, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783402421540", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
step_trainer_trusted_node1783402665180 = glueContext.getSink(path="s3://stedi-data-lake-housey/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1783402665180")
step_trainer_trusted_node1783402665180.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1783402665180.setFormat("json")
step_trainer_trusted_node1783402665180.writeFrame(SQLQuery_node1783402592909)
job.commit()