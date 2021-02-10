from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col
import pyspark.sql.functions as sf


def answer(input_file):
    """
        Énoncé : Les plus belles villes de France ont reçu une récompense de la société M&M's
        La ville d'Avignon souhaite savoir les 5 couleurs qu'elle a le *plus* reçu et leur nombre

        Hints:
        - Utilisez les fonctions DataFrame de SparkSession
    """
    # Initialiser une SparkSession
    spark = (SparkSession.builder
             .appName("exo_1")
             .getOrCreate())

    # Lire le fichier M&M data set
    mnm_file = spark.read.format("csv").option("header", "true").load(input_file)

    count_mnm_df = mnm_file \
        .filter(col('City') == 'Avignon')\
        .groupBy('City', 'Color') \
        .agg(sf.sum('Count').alias('Total')) \
        .orderBy("Total", ascending=False) \
        .limit(5)

    count_mnm_df.show()

    count_mnm_df = mnm_file \
        .filter(col('City') == 'Avignon') \
        .groupBy('City', 'Color') \
        .agg(count("Count").alias("Total")) \
        .orderBy("Total", ascending=False) \
        .limit(5)

    return count_mnm_df
