#Space for the Apache Spark script

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col
#StructType, StructField, StringType, TimestampType, DoubleType, 
from config import configuration


#Entrypoint 
def main():
    '''
    SPARK
    Create spark from a session and assing an application to it
    Will also need a Spark JAR to allow Spark to connect to Kafka with this JAR file
    Get from Maven repository => spark-sql-kafka
    
    AWS
    Will also need a package to allows Spark connect to AWS
    Get from Maven repository => hadoop-aws
    Will also need aws-java-sdk
    groupid:artefactid:version
    '''
    spark = SparkSession.builder.appName("SmartCityStreaming")\
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.1,"
            "com.amazonaws:aws-java-sdk:1.11.469")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY_ID'))\
    .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY'))\
    .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
    .getOrCreate()
            
    #Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel('WARN')
    
    #VEHICLE SCHEMA
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuel_type", StringType(), True),
        ])
    
    #GPS_SCHEMA
    gpsSchema =  StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicle_type", StringType(), True),
        
        ])
    
    #TRAFFIC CAMERA SCHEMA
    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("camera_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True),

        ])
        
    
    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),

        ])
    
    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True),
        ])
    
    
    
    
    def read_kafka_topic(topic, schema):
        return (spark.readStream.format('kafka')
        .option('kafka.bootstrap.servers', 'broker:29092')
        .option('subscribe', topic)
        .option('startingOffsets', 'earliest')
        .option('failOnDataLoss', 'false')
        .load()
        .selectExpr('CAST(value AS STRING)')
        .select(from_json(col('value'), schema).alias('data'))
        .select('data.*')
        .withWatermark('timestamp', '2 minutes')
        
        )
        
    
    def stream_writer(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
        .format('parquet')
        .option('checkpointLocation', checkpointFolder)
        .option('path', output)
        .outputMode('append')
        .start())
        
        
    
    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    emergencyDF = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    
    
    #join all dfs with id and timestamp
    
    
    #Write data to S3
    query1 = stream_writer(vehicleDF, "s3a://spark-bucket-eazi/checkpoints/vehicle_data", "s3a://spark-bucket-eazi/data/vehicle_data")
    query2 = stream_writer(gpsDF, "s3a://spark-bucket-eazi/checkpoints/gps_data", "s3a://spark-bucket-eazi/data/gps_data")
    query3 = stream_writer(trafficDF, "s3a://spark-bucket-eazi/checkpoints/traffic_data", "s3a://spark-bucket-eazi/data/traffic_data")
    query4 = stream_writer(emergencyDF, "s3a://spark-bucket-eazi/checkpoints/emergency_data", "s3a://spark-bucket-eazi/data/emergency_data")
    query5 = stream_writer(weatherDF, "s3a://spark-bucket-eazi/checkpoints/weather_data", "s3a://spark-bucket-eazi/data/weather_data")
    
    query5.awaitTermination()
    
    
    


if __name__ == '__main__':
    main()