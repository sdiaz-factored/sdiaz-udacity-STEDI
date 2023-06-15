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

# Script generated for node customers curated
customerscurated_node1686864454018 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="sdiaz",
        table_name="customer_curated",
        transformation_ctx="customerscurated_node1686864454018",
    )
)

# Script generated for node step trainer landing
steptrainerlanding_node1686864456611 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://sdiaz-lake-house/step_trainer/landing/"],
            "recurse": True,
        },
        transformation_ctx="steptrainerlanding_node1686864456611",
    )
)

# Script generated for node Join step trinaer customers curated
Joinsteptrinaercustomerscurated_node1686864465353 = Join.apply(
    frame1=customerscurated_node1686864454018,
    frame2=steptrainerlanding_node1686864456611,
    keys1=["serialnumber"],
    keys2=["serialNumber"],
    transformation_ctx="Joinsteptrinaercustomerscurated_node1686864465353",
)

# Script generated for node Drop Fields
DropFields_node1686864462649 = DropFields.apply(
    frame=Joinsteptrinaercustomerscurated_node1686864465353,
    paths=[
        "customername",
        "email",
        "phone",
        "birthday",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithfriendsasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1686864462649",
)

# Script generated for node step trainer trusted
steptrainertrusted_node1686864604728 = (
    glueContext.write_dynamic_frame.from_catalog(
        frame=DropFields_node1686864462649,
        database="sdiaz",
        table_name="step_trainer_trusted",
        transformation_ctx="steptrainertrusted_node1686864604728",
    )
)

job.commit()
