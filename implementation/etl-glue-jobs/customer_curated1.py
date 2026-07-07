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

# Script generated for node accelerometer_landing
accelerometer_landing_node1783400749521 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-lake-housey/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1783400749521")

# Script generated for node customer_trusted
customer_trusted_node1783400800106 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-data-lake-housey/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1783400800106")

# Script generated for node SQL Query
SqlQuery2961 = '''
select distinct c.*
from a
join c
on a.user=c.email
'''
SQLQuery_node1783400850019 = sparkSqlQuery(glueContext, query = SqlQuery2961, mapping = {"c":customer_trusted_node1783400800106, "a":accelerometer_landing_node1783400749521}, transformation_ctx = "SQLQuery_node1783400850019")

# Script generated for node customer_curated
EvaluateDataQuality().process_rows(frame=SQLQuery_node1783400850019, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1783400635847", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
customer_curated_node1783400943409 = glueContext.getSink(path="s3://stedi-data-lake-housey/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1783400943409")
customer_curated_node1783400943409.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
customer_curated_node1783400943409.setFormat("json")
customer_curated_node1783400943409.writeFrame(SQLQuery_node1783400850019)
job.commit()