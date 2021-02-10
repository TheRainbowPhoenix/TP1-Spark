from pyspark.sql import SparkSession
from pyspark.sql.functions import col


def answer(input_file) -> str:
    """
    Énoncé : Mathieu est père de deux enfants entre 2 et 5 ans,
    il veut regarder une série ou un film sur Netflix
    :return: Le titre de la série avec la meilleure note et qui conviennent à l'âge de ses enfants

    Le but étant de manipuler des fonctions de sélection et de projection

    Hints:
    - On se base sur la note `user_rating_score`
    - Les `rating` pour le public tout âge sont "TV-Y" et "G"
    - On ne fait pas la distinction si c'est une série pour fille ou garçon :)
    """
    print("Running answer exo 1")
    spark = (SparkSession.builder
             .appName("exo_1")
             .getOrCreate())

    netflix_shows = spark.read.format("csv").option("header", "true").load(input_file)

    is_suitable_for_children = netflix_shows.filter(col('rating') == 'G')
    show = is_suitable_for_children.filter('user_rating_score is not NULL').sort(col('user_rating_score'), ascending=False).first()

    return show.title
