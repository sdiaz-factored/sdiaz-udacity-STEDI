import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 Customer landing
S3Customerlanding_node1686854368470 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://sdiaz-lake-house/customers/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3Customerlanding_node1686854368470",
)

# Script generated for node Filter for research approval
Filterforresearchapproval_node1686853622025 = Filter.apply(
    frame=S3Customerlanding_node1686854368470,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="Filterforresearchapproval_node1686853622025",
)

# Script generated for node Store in amazon s3 trusted
Storeinamazons3trusted_node1686854702413 = glueContext.write_dynamic_frame.from_options(
    frame=Filterforresearchapproval_node1686853622025,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://sdiaz-lake-house/customers/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="Storeinamazons3trusted_node1686854702413",
)

job.commit()
