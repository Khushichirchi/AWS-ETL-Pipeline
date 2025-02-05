import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

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

# Script generated for node Album
Album_node1738603628880 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://iamspotify-project/Staging/albums.csv"], "recurse": True}, transformation_ctx="Album_node1738603628880")

# Script generated for node Tracks
Tracks_node1738603676372 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://iamspotify-project/Staging/track.csv"], "recurse": True}, transformation_ctx="Tracks_node1738603676372")

# Script generated for node Artist
Artist_node1738603673983 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://iamspotify-project/Staging/artists.csv"], "recurse": True}, transformation_ctx="Artist_node1738603673983")

# Script generated for node Join Album and Artist
JoinAlbumandArtist_node1738603799288 = Join.apply(frame1=Album_node1738603628880, frame2=Artist_node1738603673983, keys1=["artist_id"], keys2=["id"], transformation_ctx="JoinAlbumandArtist_node1738603799288")

# Script generated for node Join with tracks
Joinwithtracks_node1738604344689 = Join.apply(frame1=Tracks_node1738603676372, frame2=JoinAlbumandArtist_node1738603799288, keys1=["track_id"], keys2=["track_id"], transformation_ctx="Joinwithtracks_node1738604344689")

# Script generated for node Drop Fields
DropFields_node1738604489870 = DropFields.apply(frame=Joinwithtracks_node1738604344689, paths=["`.track_id`", "id"], transformation_ctx="DropFields_node1738604489870")

# Script generated for node Destination
EvaluateDataQuality().process_rows(frame=DropFields_node1738604489870, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1738603583638", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
Destination_node1738604588752 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1738604489870, connection_type="s3", format="glueparquet", connection_options={"path": "s3://iamspotify-project/data_warehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="Destination_node1738604588752")

job.commit()