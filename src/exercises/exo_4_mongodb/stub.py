from pyspark.sql import SparkSession


def answer():
    """

    :return: `centenarians` La liste de personnages qui ont plus de 100 ans

    Pour lancer l'application
    spark-submit \
    --conf "spark.mongodb.input.uri=mongodb://localhost:27017/test.myCollection?readPreference=primaryPreferred" \
    --conf "spark.mongodb.output.uri=mongodb://localhost:27017/test.myCollection" \
    --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.0 src/exercises/exo_4_mongodb/stub.py
    """
    # Rajouter les configs dans l'instanciation de SparkSession pour utiliser le connecteur MongoDB
    # `spark.mongodb.input.uri=mongodb://localhost:27017/test.myCollection?readPreference=primaryPreferred`
    # `spark.mongodb.output.uri=mongodb://localhost:27017/test.myCollection`
    # `spark.jars.packages=org.mongodb.spark:mongo-spark-connector_2.12:3.0.0`
    ss = SparkSession.builder.appName("Exo Mongo")
        ...
        .getOrCreate()

    characters_name_and_age = [("Bilbo Baggins", 50), ("Thorin", 195), ("Balin", 178), ("Kili", 77), ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)]

    # Créer un DataFrame et l'écrire dans MongoDB
    centenarians = ss.sql("SELECT name, age FROM characters WHERE age >= 100")
    
    return centenarians.collect()