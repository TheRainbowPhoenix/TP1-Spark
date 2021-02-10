# TP 1 
*tousse anssanble*

** EXO 1 **

text_file = sc.textFile(input_file)
# Utiliser les fonctions RDD avec `text_file` pour splitter les mots et compter chacun des mots
sorted_counts = text_file.flatMap(lambda line: filter(None, line.split()))\
    .map(lambda word: (word, 1))\
    .reduceByKey(lambda a,b:a +b)\
    .map(lambda a:(a[1],a[0]))\
    .sortByKey(False,1) \
    .map(lambda a: (a[1], a[0]))
    # Retourner le mot le plus fréquent du dataset
most_frequent_word = sorted_counts.first()

** EXO 2 **

netflix_shows = spark.read.format("csv").option("header", "true").load(input_file)

netflix_shows.printSchema()

is_suitable_for_children = netflix_shows.filter(col('rating') == 'G')
is_suitable_for_children.show(truncate=False)
show = is_suitable_for_children.filter('user_rating_score is not NULL').sort(col('user_rating_score'), ascending=False).first()

return show.title

** EXO 3 **
// Cet exercice est sponso par Amazon Cloud Prime. Avec amazon cloud prime, plus besoin de pc, car tout est dans le cloud !


## Des examples
https://sparkbyexamples.com/pyspark/pyspark-where-filter/


# Spark sur les machines du LIA
* Tous les logiciels demandés sont installés dans la version demandée. Ils sont installés uniquement sur la machine de test / démo qui se trouve dans la salle mc114 du LIA ;

* Spark est installé dans /usr/local/stow/spark-3.0.1.
** Les journaux (SPARK_LOG_DIR) seront stockés dans ~/spark/logs (homedir / dossier personnel de l'utilisateur) ;
** Le dossier de travail (SPARK_WORKER_DIR) sera ~/spark/work (homedir / dossier personnel de l'utilisateur) ;
** J'ai testé le bon fonctionnement avec les commandes
*** « /usr/local/stow/spark-3.0.1/bin/run-example SparkPi 4 » ;
*** spark-shell

# Accéder au MongoDB de l'université via VPN
mongo --host pedago.univ-avignon.fr

# Java sur pedago
mkdir spark && cd spark
wget https://github.com/AdoptOpenJDK/openjdk11-binaries/releases/download/jdk-11.0.10%2B9/OpenJDK11U-jre_x64_linux_hotspot_11.0.10_9.tar.gz
tar xzvf OpenJDK11U-jre_x64_linux_hotspot_11.0.10_9.tar.gz
chmod +x jdk-11.0.10+9-jre/bin/*
jdk-11.0.10+9-jre/bin/java -version
export PATH=$PWD/jdk-11.0.10+9-jre/bin:$PATH

java -version

# Spark sur pedago
wget https://downloads.apache.org/spark/spark-3.0.1/spark-3.0.1-bin-hadoop2.7.tgz
tar xzvf spark-3.0.1-bin-hadoop2.7.tgz
chmod +x spark-3.0.1-bin-hadoop2.7/bin/*
spark-3.0.1-bin-hadoop2.7/bin/spark-shell

# No module named 'src
Ajouter le projet dans le pythonpath
PYTHONPATH=/home/nas-wks01/users/uapv1701834/study/Data_Analytics/TP3/src/:$PYTHONPATH