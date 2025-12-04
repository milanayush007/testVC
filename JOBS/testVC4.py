import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from common_lib.common_utils import log_message, validate_data

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

# Script generated for node Amazon S3
AmazonS3_node1764685379951 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://qr-dev-etl-s3/test_folder_version_control/data/"], "recurse": True}, transformation_ctx="AmazonS3_node1764685379951")

# Validate data
log_message("Validating data from Amazon S3")
validate_data(AmazonS3_node1764685379951)

# Script generated for node Change Schema
ChangeSchema_node1764687361496 = ApplyMapping.apply(frame=AmazonS3_node1764685379951, mappings=[("npi", "string", "npi", "string"), ("provider_type", "string", "provider_type", "string"), ("first_name", "string", "first_name", "string"), ("last_name", "string", "last_name", "string"), ("organization_name", "string", "organization_name", "string"), ("hco_primary_npi", "string", "hco_primary_npi", "string"), ("primary_specialty", "string", "primary_specialty", "string"), ("secondary_specialty", "string", "secondary_specialty", "string"), ("provider_phone_number", "string", "provider_phone_number", "string"), ("provider_address", "string", "provider_address", "string"), ("provider_city", "string", "provider_city", "string"), ("provider_state", "string", "provider_state", "string"), ("provider_zip", "string", "provider_zip", "string")], transformation_ctx="ChangeSchema_node1764687361496")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1764687361496, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1764683860098", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1764687375145 = glueContext.write_dynamic_frame.from_options(frame=ChangeSchema_node1764687361496, connection_type="s3", format="glueparquet", connection_options={"path": "s3://qr-dev-etl-s3/test_folder_version_control/output/1/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1764687375145")

job.commit()