import pyspark


def answer(input_file) -> tuple:
    """
    Énoncé : Avec le récit en donnée source `data/lupin.txt`, trouver le mot le plus fréquent

    Le but étant de manipuler les fonctions basiques `map` et `reduce`

    :return: `most_frequent_word` le mot le plus fréquent au format tuple (mot, occurrence)

    Hints:
    - On se base sur la note `user_rating_score`
    - Les `rating` pour le public tout âge sont "TV-Y" et "G"
    - On ne fait pas la distinction si c'est une série pour fille ou garçon :)
    """
    print("Running answer exo 1")
    sc = pyspark.SparkContext(appName="Exo 1", master="local[*]").getOrCreate()

    # Importer le fichier texte `input_file`
    text_file = sc.textFile(input_file)
    # Utiliser les fonctions RDD avec `text_file` pour splitter les mots et compter chacun des mots
    sorted_counts = text_file.flatMap(lambda line: filter(None, line.split())) \
        .map(lambda word: (word, 1)) \
        .reduceByKey(lambda a, b: a + b) \
        .map(lambda a: (a[1], a[0])) \
        .sortByKey(False, 1) \
        .map(lambda a: (a[1], a[0]))
    # Retourner le mot le plus fréquent du dataset
    most_frequent_word = sorted_counts.first()

    return most_frequent_word
