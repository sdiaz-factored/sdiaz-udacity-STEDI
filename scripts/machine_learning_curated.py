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

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1686865163432 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="sdiaz",
        table_name="step_trainer_trusted",
        transformation_ctx="step_trainer_trusted_node1686865163432",
    )
)

# Script generated for node customer trusted
customertrusted_node1686865184114 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="sdiaz",
        table_name="customer_trusted",
        transformation_ctx="customertrusted_node1686865184114",
    )
)

# Script generated for node accelorometer trusted
accelorometertrusted_node1686865246210 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="sdiaz",
        table_name="accelerometer_trusted",
        transformation_ctx="accelorometertrusted_node1686865246210",
    )
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1686865360984 = ApplyMapping.apply(
    frame=customertrusted_node1686865184114,
    mappings=[
        ("customername", "string", "`(right) customername`", "string"),
        ("email", "string", "`(right) email`", "string"),
        ("phone", "string", "`(right) phone`", "string"),
        ("birthday", "string", "`(right) birthday`", "string"),
        ("serialnumber", "string", "`(right) serialnumber`", "string"),
        ("registrationdate", "long", "`(right) registrationdate`", "long"),
        ("lastupdatedate", "long", "`(right) lastupdatedate`", "long"),
        (
            "sharewithresearchasofdate",
            "long",
            "`(right) sharewithresearchasofdate`",
            "long",
        ),
        (
            "sharewithfriendsasofdate",
            "long",
            "`(right) sharewithfriendsasofdate`",
            "long",
        ),
        (
            "sharewithpublicasofdate",
            "long",
            "`(right) sharewithpublicasofdate`",
            "long",
        ),
    ],
    transformation_ctx="RenamedkeysforJoin_node1686865360984",
)

# Script generated for node Join step trainer and customers
Joinsteptrainerandcustomers_node1686865419350 = Join.apply(
    frame1=RenamedkeysforJoin_node1686865360984,
    frame2=step_trainer_trusted_node1686865163432,
    keys1=["`(right) serialnumber`"],
    keys2=["serialnumber"],
    transformation_ctx="Joinsteptrainerandcustomers_node1686865419350",
)

# Script generated for node Drop Fields
DropFields_node1686865494545 = DropFields.apply(
    frame=Joinsteptrainerandcustomers_node1686865419350,
    paths=[
        "`(right) customername`",
        "`(right) phone`",
        "`(right) birthday`",
        "`(right) serialnumber`",
        "`(right) registrationdate`",
        "`(right) lastupdatedate`",
        "`(right) sharewithresearchasofdate`",
        "`(right) sharewithfriendsasofdate`",
        "`(right) sharewithpublicasofdate`",
    ],
    transformation_ctx="DropFields_node1686865494545",
)

# Script generated for node Join accelerometer and step trainer data
Joinaccelerometerandsteptrainerdata_node1686865579526 = Join.apply(
    frame1=DropFields_node1686865494545,
    frame2=accelorometertrusted_node1686865246210,
    keys1=["`(right) email`", "sensorreadingtime"],
    keys2=["user", "timestamp"],
    transformation_ctx="Joinaccelerometerandsteptrainerdata_node1686865579526",
)

# Script generated for node Drop Fields
DropFields_node1686865602953 = DropFields.apply(
    frame=Joinaccelerometerandsteptrainerdata_node1686865579526,
    paths=["`(right) email`", "timestamp"],
    transformation_ctx="DropFields_node1686865602953",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1686865866583 = (
    glueContext.write_dynamic_frame.from_catalog(
        frame=DropFields_node1686865602953,
        database="sdiaz",
        table_name="machine_learning_curated",
        transformation_ctx="AWSGlueDataCatalog_node1686865866583",
    )
)

job.commit()
