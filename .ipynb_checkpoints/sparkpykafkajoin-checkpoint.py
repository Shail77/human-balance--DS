from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

#  New Topic: Write a spark script `sparkpykafkajoin.py` to join the customer dataframe and the customer risk
#  dataframes, joining on the email address. Create a JSON output to the newly created kafka topic you configured for
#  STEDI to subscribe to that contains at least the fields below:
#
# ```json
# {
#     "customer": "Santosh.Fibonnaci@test.com",
#     "score": "28.5",
#     "email": "Santosh.Fibonnaci@test.com",
#     "birthYear": "1963"
# }


'''
with this JSON format:
{
    "key":"Q3VzdG9tZXI=",
    "existType":"NONE",
    "Ch":false,
    "Incr":false,
    "zSetEntries":[
        {
            "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
            "Score":0.0
        }
    ],
    "zsetEntries":[
        {
            "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
            "score":0.0
        }
    ]
}
'''

# create a StructType for the Kafka redis-server topic which has all changes made to Redis
kafkaRedisSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("existType", StringType()),
        StructField("Ch", BooleanType()),
        StructField("Incr", BooleanType()),
        StructField("zSetEntries", ArrayType(
            StructType([
                StructField("element", StringType()),
                StructField("Score", StringType())
            ]))
                    )
    ]
)

# create a StructType for the Customer JSON that comes from Redis
kafkaJSONSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", StringType()),
        StructField("email", StringType()),
        StructField("birthYear", StringType())
    ]
)

# create a StructType for the Kafka stedi-events topic which has the Customer Risk JSON that comes from Redis
kafkaEventschema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ]
)

# create a spark application object and set the log level to WARN
spark = SparkSession.builder.appName("STEDI").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# using the spark application object, read a streaming dataframe from the Kafka topic redis-server as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before you started the spark stream
kafkaRedisDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

# cast the value column in the streaming dataframe as a STRING
kafkaRedisDF = kafkaRedisDF.selectExpr("cast(value as string) value")

# parse the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"key":"Q3..|
# +------------+
#


# (Note: The Redis Source for Kafka has redundant fields zSetEntries and zsetentries, only one should be parsed)
#
# and create separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+


# storing them in a temporary view called RedisSortedSet
kafkaRedisDF.withColumn("value", from_json("value", kafkaRedisSchema)) \
    .select(col('value.existType'), col('value.Ch'),
            col('value.Incr'), col('value.zSetEntries')) \
    .createOrReplaceTempView("RedisSortedSet")

# execute a sql statement against a temporary view, which statement takes the element field from the 0th element in the array of structs and create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against a view is different than a dataframe, and it makes it easy to select the nth element of an array in a sql column
zSetEntriesEncodedStreamingDF = spark.sql("select zSetEntries[0].element as encodedCustomer from RedisSortedSet")

# TO-DO: take the encodedCustomer column which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |[7B 22 73 74 61 7...|
# +--------------------+

# and convert it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"customerName":"...|
# +--------------------+


'''
with this JSON format:
{
    "customerName":"Sam Test",
    "email":"sam.test@test.com",
    "phone":"8015551212",
    "birthDay":"2001-01-03"
}
'''

zSetDecodedEntriesStreamingDF = zSetEntriesEncodedStreamingDF.withColumn("customer", unbase64(
    zSetEntriesEncodedStreamingDF.encodedCustomer).cast("string"))

# TO-DO: parse the JSON in the Customer record and store in a temporary view called CustomerRecords
customerSchema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
    ]
)
zSetDecodedEntriesStreamingDF.withColumn("customer", from_json("customer", customerSchema)) \
    .select(col('customer.*')) \
    .createOrReplaceTempView("CustomerRecords")

# TO-DO: JSON parsing will set non-existent fields to null, so let's select just the fields we want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF
emailAndBirthDayStreamingDF = spark.sql(
    "SELECT * FROM CustomerRecords WHERE email IS NOT NULL AND birthDay IS NOT NULL")

# TO-DO: Split the birth year as a separate field from the birthday
emailAndBirthDayStreamingDF = emailAndBirthDayStreamingDF.withColumn('birthYear',
                                                                     split(emailAndBirthDayStreamingDF.birthDay,
                                                                           "-").getItem(0))

# Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(col('email'), col('birthYear'))

# using the spark application object, read a streaming dataframe from the Kafka topic stedi-events as the source
# Be sure to specify the option that reads all the events from the topic including those that were published before
# you started the spark stream
kafkaEventsDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

# cast the value column in the streaming dataframe as a STRING
kafkaEventsDF = kafkaEventsDF.selectExpr("cast(value as string) value")

# TO-DO: parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+


# storing them in a temporary view called CustomerRisk
kafkaEventsDF.withColumn("value", from_json("value", kafkaEventschema)) \
    .select(col('value.customer'), col('value.score'), col('value.riskDate')) \
    .createOrReplaceTempView("CustomerRisk")

# TO-DO: execute a sql statement against a temporary view, selecting the customer and the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")

# join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe
stediScoreStreamingDF = customerRiskStreamingDF.join(emailAndBirthYearStreamingDF, expr("customer = email"))



# sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+
# In this JSON Format {"customer":"Santosh.Fibonnaci@test.com","score":"28.5","email":"Santosh.Fibonnaci@test.com","birthYear":"1963"}


# stediScoreStreamingDF.writeStream.outputMode("append").format("console").start().awaitTermination()

#stediScoreStreamingDF.selectExpr("CAST(customer AS STRING) AS key", "to_json(struct(*)) AS value") \
#     .writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "localhost:9092") \
#     .option("topic", "risk-topic") \
#     .option("checkpointLocation", "/tmp/kafka-checkpoint-2") \
#     .option("failOnDataLoss", "false") \
#     .start() \
#     .awaitTermination()

stedi_score_streaming_df_kafka_query = stediScoreStreamingDF.selectExpr("CAST(customer AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "risk-topic") \
    .option("checkpointLocation", "/tmp/kafka-checkpoint-100") \
    .option("failOnDataLoss", "false") \
    .start()

stedi_score_streaming_df_console_query = stediScoreStreamingDF.writeStream.outputMode("append").format("console").start()

stedi_score_streaming_df_kafka_query.awaitTermination()
stedi_score_streaming_df_console_query.awaitTermination()