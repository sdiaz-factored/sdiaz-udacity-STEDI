import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node customers trusted
customerstrusted_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="sdiaz",
    table_name="customer_trusted",
    transformation_ctx="customerstrusted_node1",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1686863494597 = glueContext.create_dynamic_frame.from_catalog(
    database="sdiaz",
    table_name="accelerometer_landing",
    transformation_ctx="accelerometertrusted_node1686863494597",
)

# Script generated for node Join
Join_node1686863607587 = Join.apply(
    frame1=accelerometertrusted_node1686863494597,
    frame2=customerstrusted_node1,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1686863607587",
)

# Script generated for node Drop Fields
DropFields_node1686863742853 = DropFields.apply(
    frame=Join_node1686863607587,
    paths=["user", "x", "timestamp", "y", "z"],
    transformation_ctx="DropFields_node1686863742853",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1686863943986 = glueContext.write_dynamic_frame.from_catalog(
    frame=DropFields_node1686863742853,
    database="sdiaz",
    table_name="customer_curated",
    transformation_ctx="AWSGlueDataCatalog_node1686863943986",
)

job.commit()
