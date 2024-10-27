from pyspark.sql import SparkSession

# Initialiser la session Spark avec support S3
spark = SparkSession.builder \
    .appName("S3A Example") \
    .config("spark.hadoop.fs.s3a.access.key", "") \
    .config("spark.hadoop.fs.s3a.secret.key", "") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .getOrCreate()

# Lire les données depuis S3
s3_path = "s3a://FilmsData.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_path)

# Créer une table temporaire
df.createOrReplaceTempView("films")

# Exécuter une analyse avec Spark SQL
result = spark.sql("""
    SELECT title, press_rate, spectateur_rate, duree_minutes
    FROM films
    WHERE press_rate > 3.0
    ORDER BY spectateur_rate DESC
""")
result.show()



# Calculer la moyenne des notes de presse et de spectateurs
average_ratings = spark.sql("""
    SELECT 
        AVG(press_rate) AS avg_press_rate, 
        AVG(spectateur_rate) AS avg_spectator_rate
    FROM films
""")
average_ratings.show()


# Top 5 des films les mieux notés par les spectateurs
top_spectator_rated = spark.sql("""
    SELECT 
        title, 
        spectateur_rate 
    FROM films 
    ORDER BY spectateur_rate DESC 
    LIMIT 5
""")
top_spectator_rated.show()



# Classer les films par catégories de durée et compter le nombre de films dans chaque catégorie
duration_category = spark.sql("""
    SELECT 
        CASE 
            WHEN duree_minutes < 90 THEN 'Short'
            WHEN duree_minutes BETWEEN 90 AND 120 THEN 'Medium'
            ELSE 'Long'
        END AS duration_category, 
        COUNT(*) AS film_count
    FROM films
    GROUP BY duration_category
""")
duration_category.show()

# Importer les fonctions de corrélation
from pyspark.sql.functions import corr

# Calculer la corrélation entre les notes de presse et les notes de spectateurs
correlation = df.select(corr("press_rate", "spectateur_rate").alias("correlation")).show()



# Classer les films par catégories de notes de presse et de spectateurs
rating_distribution = spark.sql("""
    SELECT 
        CASE 
            WHEN press_rate < 2 THEN 'Low'
            WHEN press_rate BETWEEN 2 AND 3.5 THEN 'Medium'
            ELSE 'High'
        END AS press_rate_category,
        CASE 
            WHEN spectateur_rate < 2 THEN 'Low'
            WHEN spectateur_rate BETWEEN 2 AND 3.5 THEN 'Medium'
            ELSE 'High'
        END AS spectator_rate_category,
        COUNT(*) AS film_count
    FROM films
    GROUP BY press_rate_category, spectator_rate_category
""")
rating_distribution.show()


# Fermer la session
spark.stop()
