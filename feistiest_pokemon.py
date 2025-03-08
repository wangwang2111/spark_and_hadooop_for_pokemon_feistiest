from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, coalesce, when, row_number, lit
from pyspark.sql.window import Window

# Initialize Spark Session with HDFS configuration
spark = SparkSession.builder \
    .appName("FeistiestPokemonAnalysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
    .getOrCreate()

# Define Input & Output Paths (HDFS)
input_path = "/spark/pokemon.csv"  # Relative to HDFS root
output_path = "/spark/job-output/feistiest_pokemon.csv"

# Load the dataset from HDFS
try:
    df = spark.read.csv(input_path, header=True, inferSchema=True)
except Exception as e:
    print(f"Error loading data: {str(e)}")
    spark.stop()
    exit(1)

# Clean Data & Compute Feistiness
df = df.withColumn("type2", coalesce(col("type2"), lit("NA")))  # Changed here

# Compute feistiness
df = df.withColumn("feistiness", 
                   round(col("attack") / when(col("weight_kg") > 0, col("weight_kg")).otherwise(1), 2))

# Define Window Partition for Ranking
windowSpec = Window.partitionBy("type1").orderBy(col("feistiness").desc())

# Assign Rank & Filter the Feistiest Pokémon per type1
feistiest_pokemon = df.withColumn("rank", row_number().over(windowSpec)) \
                      .filter(col("rank") == 1) \
                      .select("type1", "type2", "name", "feistiness")

# Save the output to HDFS with header
try:
    feistiest_pokemon.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(output_path)
except Exception as e:
    print(f"Error writing output: {str(e)}")

# Stop Spark Session
spark.stop()