import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer landing
accelerometerlanding_node1686857432659 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://sdiaz-lake-house/accelerometer/landing/"],
            "recurse": True,
        },
        transformation_ctx="accelerometerlanding_node1686857432659",
    )
)

# Script generated for node customers trusted
customerstrusted_node1686857070059 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://sdiaz-lake-house/customers/trusted/"],
            "recurse": True,
        },
        transformation_ctx="customerstrusted_node1686857070059",
    )
)

# Script generated for node Join accelerometer landing and customer trusted
Joinaccelerometerlandingandcustomertrusted_node1686855680544 = Join.apply(
    frame1=customerstrusted_node1686857070059,
    frame2=accelerometerlanding_node1686857432659,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Joinaccelerometerlandingandcustomertrusted_node1686855680544",
)

# Script generated for node Drop Fields
DropFields_node1686855775357 = DropFields.apply(
    frame=Joinaccelerometerlandingandcustomertrusted_node1686855680544,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
        "email",
        "customerName",
    ],
    transformation_ctx="DropFields_node1686855775357",
)

# Script generated for node accelerometer trusted
accelerometertrusted_node1686857320931 = (
    glueContext.write_dynamic_frame.from_options(
        frame=DropFields_node1686855775357,
        connection_type="s3",
        format="json",
        connection_options={
            "path": "s3://sdiaz-lake-house/accelerometer/trusted/",
            "partitionKeys": [],
        },
        transformation_ctx="accelerometertrusted_node1686857320931",
    )
)

job.commit()
