import pyspark

# On initialise `sc` explicitement vu que nous ne sommes pas dans un spark-shell
sc = pyspark.SparkContext(appName="exo_0").getOrCreate()


def answer():
    nums = [1, 2, 3, 4]
    rdd_nums = sc.parallelize(nums)
    return rdd_nums
