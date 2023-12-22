def s3_spark():
        from pyspark.sql import SparkSession
        import pandas as pd
        from dotenv import load_dotenv
        from pyspark.sql import functions as F
        import boto3
        import os
        from io import BytesIO
        import pyspark

        load_dotenv()

        aws_access_key_id = os.environ['AWS_ACCESS_KEY']
        aws_secret_access_key = os.environ['AWS_SECRET_ACCESS_KEY']
        region_name = os.environ['AWS_REGION']
        aws_bucket = os.environ['S3_BUCKET_NAME']

        # os.environ['PYSPARK_SUBMIT_ARGS']='--packages org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.271'
        # os.environ['JAVA_HOME']='/usr/bin/java'

        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)


        access_key = aws_access_key_id
        secret_key = aws_secret_access_key 

        # spark = SparkSession.builder \
        #     .appName("batch_processing_spark_s3_to_spark") \
        #     .master('local[*]') \
        #     .config("spark.hadoop.fs.s3a.access.key", access_key) \
        #     .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        #     .config("com.amazonaws.services.s3.enableV4", "true") \
        #     .config('fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        #     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle-1.12.576")\
        #     .getOrCreate()

        # Create a Spark session with the security keys
        # spark = SparkSession.builder \
        #     .appName("batch_processing_spark_s3_to_spark") \
        #     .master('local[*]') \
        #     .config("spark.hadoop.fs.s3a.access.key", access_key) \
        #     .config("spark.hadoop.fs.s3a.secret.key", secret_key) \
        #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        #     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.576")\
        #     .config("com.amazonaws.services.s3.enableV4", "true") \
        #     .getOrCreate()
        
        conf= pyspark.SparkConf()\
        .setMaster("local[*]")\
        .setAppName("batch_processing_spark_s3_to_spark")

        spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()

        hadoopConf=spark._jsc.hadoopConfiguration()
        hadoopConf.set('fs.s3a.access.key', access_key)
        hadoopConf.set('fs.s3a.secret.key', secret_key)
        hadoopConf.set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
        hadoopConf.set("com.amazonaws.services.s3.enableV4", "true")
        #hadoopConf.set('fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider')
        hadoopConf.set('fs.s3a.aws.credentials.provider','org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider')

            
        df = spark.read.json("s3a://ocado-bucket/user_signons_/*.json")



        df.show(3)

        # df = df.withColumn('dob', F.to_date(df.dob, 'dd-MM-yyyy'))

        df_p = df.toPandas()

        print('loading data into s3.....')
        csv_buffer = BytesIO()
        df_p.to_parquet(csv_buffer, index=False)  

    
        # Upload the file
        # s3.upload_file(local_file_path, S3_BUCKET_NAME, s3_key_name)
        s3.put_object(Bucket=aws_bucket, Key='user_signons_cleaned/user_signons_.parquet', Body=csv_buffer.getvalue())
        print('loaded data into s3')

s3_spark()