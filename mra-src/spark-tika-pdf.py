from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from tika import parser


def udf_bin_to_text_using_tika(data):
    parsed = parser.from_buffer(data)
    return parsed['content']


if __name__ == '__main__':
    # Spark Object Initialization
    sparkSession = SparkSession. \
        builder. \
        appName("Spark-Tika-Pdf"). \
        master("local[*]"). \
        getOrCreate()

    # Path of the file.
    file_path = "/Users/maheshvakkund/Documents/PYTHON/spark-mra-poc/mra-resources/ELSS_FY 2022-23.pdf"

    # Read the Pdf files in binary format and convert a dataframe
    read_data_df = sparkSession. \
        sparkContext. \
        binaryFiles(file_path). \
        toDF()

    # Renaming the columns
    read_data_df = read_data_df. \
        withColumnRenamed("_1", "path")

    # Renaming the columns
    read_data_df = read_data_df. \
        withColumnRenamed("_2", "data")

    # Udf to convert binary to text
    udf_bin_to_text_using_tika_v = udf(lambda z: udf_bin_to_text_using_tika(z), StringType())
    final_df = read_data_df. \
        select("data"). \
        withColumn("text_data", udf_bin_to_text_using_tika_v(col("data")))

    final_df.select("text_data").show(truncate=False)
