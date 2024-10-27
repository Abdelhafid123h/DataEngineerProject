from pyspark.sql import SparkSession
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.functions import col

# Initialiser la session Spark avec support Hive
spark = SparkSession.builder \
    .appName("KMeansClustering") \
    .config("spark.hadoop.fs.s3a.access.key", "") \
    .config("spark.hadoop.fs.s3a.secret.key", "") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
    .enableHiveSupport() \
    .getOrCreate()

# Lire les données depuis S3
s3_path = "s3a://file.csv"
df = spark.read.option("header", "true").option("inferSchema", "true").csv(s3_path)
df = df.withColumn("duree_minutes", col("duree_minutes").cast("double"))
df = df.na.drop()
# Assembler les colonnes en un vecteur de caractéristiques pour le clustering
assembler = VectorAssembler(inputCols=["press_rate", "spectateur_rate", "duree_minutes"], outputCol="features")
assembler = VectorAssembler(
    inputCols=["press_rate", "spectateur_rate", "duree_minutes"],
    outputCol="features",
    handleInvalid="skip"  # or "keep"
)
dataset = assembler.transform(df)

# Validation croisée pour déterminer le meilleur k
best_k = 2
best_score = -1  # Initialiser pour rechercher le score de silhouette maximal

for k in range(2, 11):
    kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
    model = kmeans.fit(dataset)
    predictions = model.transform(dataset)
    
    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(predictions)
    
    print(f"Pour k = {k}, score de silhouette = {silhouette}")
    
    if silhouette > best_score:
        best_score = silhouette
        best_k = k

print(f"Meilleur nombre de clusters (k) : {best_k} avec un score de silhouette de {best_score}")

# Appliquer KMeans avec le meilleur k pour étiqueter les données
kmeans_final = KMeans().setK(best_k).setSeed(1).setFeaturesCol("features")
model_final = kmeans_final.fit(dataset)

# Créer les prédictions et ajouter une colonne 'cluster' aux données d'origine
clustered_data = model_final.transform(dataset)

# Créer une table Hive et y écrire les données (remplacez 'database_name' et 'table_name' par vos propres noms)
spark.sql("CREATE DATABASE IF NOT EXISTS database_name")
clustered_data.write.mode("overwrite").saveAsTable("database_name.table_name")

# Vérifier que les données ont bien été enregistrées
spark.sql("SELECT * FROM database_name.table_name").show()

# Fermer la session
spark.stop()
