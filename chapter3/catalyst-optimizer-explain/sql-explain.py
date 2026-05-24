from pyspark.sql import SparkSession

if __name__ == "__main__":

    spark = (SparkSession
             .builder
             .appName("PythonMnMCount")
             .getOrCreate())

    mnm_df = (spark.read.format("csv")
              .option("header", "true")
              .option("inferSchema", "true")
              .load("../../chapter2-mnmcount/mnm_dataset.csv"))

    ca_count_mnm_df = (mnm_df
                       .select("State", "Color", "Count")
                       .where(mnm_df.State == "CA")
                       .groupBy("State", "Color")
                       .sum("Count")
                       .orderBy("sum(Count)", ascending=False))

    ca_count_mnm_df.explain(True)

    ca_count_mnm_df.show(n=10, truncate=False)
    # Stop the SparkSession
    spark.stop()