import pyspark
from pyspark.sql import SQLContext
from pyspark.sql.functions import substring

"""
    A simple script exploring examples of different spark partitioners on structure of resulting partitions.
"""

csvFile = "example.csv"
SMALL_NUM_REPARTITIONS = 4      # Choose how many partitions to use in each repartitioning strategy for small examples.
LARGE_NUM_REPARTITIONS = 10000  # Choose how many partitions to use in each repartitioning strategy for large examples.
LARGE_NUM_RECORDS = 1000000     # Choose how many records from the large, fake CA file to use (max: approx 55 million).

def quiet_logs(sc):
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )

def smallRddExample(sc):
    """
        NOTE: not complete!! Just a placeholder.

        A small-scale illustration of different rdd partitioning strategies. Effects directly demonstrated w/ glom().collect().
    """

    data = [float(i) for i in range(20)]
    rdd0 = sc.parallelize(data, SMALL_NUM_REPARTITIONS)
    rddCount = rdd0.count()
    print(f"rdd0.count() = {rddCount} w/ numParts {rdd0.getNumPartitions()}")

def smallDataframeExample():
    """
        A small-scale illustration of different dataframe partitioning strategies. Effects directly demonstrated w/ glom().collect().
    """

    sqlContext = SQLContext(sc)
    print(f"Loading {csvFile} into dataframe...")
    df = sqlContext.read.format("csv").option("header", "true").load(csvFile)
    df.printSchema()

    df = df.withColumn("tractCode", substring("geocode",0,2))
    glommedRdd = df.rdd.glom()
    rdd = glommedRdd.collect()
    print(f"--------- The initial df.rdd started w/ {df.rdd.getNumPartitions()} partitions: -------------")
    for partition in rdd:
        print(partition)

    print(f"REPARTITIONING RDD TO {SMALL_NUM_REPARTITIONS} partitions BASED ON default hashCode!")
    df2 = df.repartition(SMALL_NUM_REPARTITIONS)
    glommedRdd = df2.rdd.glom()
    rdd = glommedRdd.collect()
    print(f"--------- The df2, rdd contains {df2.rdd.getNumPartitions()} partitions: -------------")
    for partition in rdd:
        print(partition)

    print(f"ALTERNATIVE REPARTITIONING TO {SMALL_NUM_REPARTITIONS} partitions BASED ON tractCode!")
    df3 = df.repartition(SMALL_NUM_REPARTITIONS, 'tractCode').drop('tractCode')
    glommedRdd = df3.rdd.glom()
    rdd = glommedRdd.collect()
    print(f"--------- The df3.rdd contains {df3.rdd.getNumPartitions()} partitions: -------------")
    for partition in rdd:
        print(partition)

    print(f"ALTERNATIVE REPARTITIONING TO {SMALL_NUM_REPARTITIONS} partitions BASED ON rangepartition of tractCode!")
    df4 = df.repartitionByRange(SMALL_NUM_REPARTITIONS, 'tractCode').drop('tractCode')
    glommedRdd = df4.rdd.glom()
    rdd = glommedRdd.collect()
    print(f"--------- The df4.rdd contains {df4.rdd.getNumPartitions()} partitions: -------------")
    for partition in rdd:
        print(partition)

def largeDataframeExample():
    """
        This example is too big for prints of glom().collect() to work, so we instead measure:

            - On avg for each geolevel, how many partitions was each tract, county, or block code in?
            - How many partitions were empty?
            - How many partitions were there in total?
            - What was the distribution of partition sizes?
    """

def main():
    sc = pyspark.SparkContext()
    quiet_logs(sc)

    #smallRddExample(sc)
    #smallDataframeExample()
    #largeRddExample()
    largeDataframeExample()

if __name__ == "__main__":
    main()
