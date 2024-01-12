# -*- coding: utf-8 -*-
"""

# **Projet Création d'une base d'apprentissage ML à partir de données brutes**
"""
import argparse
import pyspark.sql.functions as f
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.window import Window
import findspark
import io
import json
from datetime import datetime

def add_log(log_message):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"{timestamp} - {log_message}")

add_log("démarrage du script d'extraction")    

# Récupération des arguments
parser = argparse.ArgumentParser(description='Spark Job Parameters')
parser.add_argument('--DESTINATION', type=str, default='gs://projets_blent/ecom/out/', help='Destination for the output table in CSV format')
parser.add_argument('--DATE_START', type=str, required=True, help='Starting date for event retrieval (format: YYYY-MM-DD)')
parser.add_argument('--DATE_END', type=str, required=True, help='End date for event retrieval (format: YYYY-MM-DD)')
parser.add_argument('--GCS_KEY_FILE_PATH', type=str, default="/home/admin/gsc_key.txt", help='Key File Path GCS')
parser.add_argument('--GCS_SOURCE', type=str,  default="gs://projets_blent/ecom/blent-learning-user-ressources.s3.eu-west-3.amazonaws.com/projects/9c15cb", help='Data source Path')
args = parser.parse_args()

#====== PARAMETRES ====== 
DATE_START=args.DATE_START
DATE_END=args.DATE_END
DESTINATION=args.DESTINATION #Path et fileName destination
GCS_KEY_FILE_PATH=args.GCS_KEY_FILE_PATH 
SOURCE=args.GCS_SOURCE

# Configuration de la session Spark
spark = SparkSession.builder.appName("SparkJobEcom").getOrCreate()
spark = SparkSession \
    .builder \
    .appName("PySpark") \
    .getOrCreate()


# Charger le contenu du fichier JSON key fileGCS
with open(GCS_KEY_FILE_PATH, 'r') as fichier:
    keyfile = json.load(fichier)
    
# Configuration de Hadoop pour l'authentification GCS 
#spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile","/home/admin/gsc_key.txt")
spark._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.private.key.id",  keyfile["private_key_id"])
spark._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.private.key", keyfile["private_key"])
spark._jsc.hadoopConfiguration().set("fs.gs.auth.service.account.email",  keyfile["client_email"])

sc = spark.sparkContext
sql_c = SQLContext(spark.sparkContext)

# Chargements des fichiers csv du bucket
data = spark.read.csv(SOURCE+"/2019-Oct.csv",
                      header=True)

data.show(5,truncate=False)
#add_log( str(data.count()) + ' lignes dans les fichiers')

#conversion des colonnes et filtre sur les dates spécifiés en paramètres
data = data.withColumn("event_time", f.col("event_time").cast("timestamp"))\
           .filter( ( f.col("event_time")>= DATE_START) &  ( f.col("event_time") <= DATE_END ) )\
           .withColumn("product_id", f.col("product_id").cast("int"))\
           .withColumn("category_id", f.col("product_id").cast("int"))\
           .withColumn("price", f.col("price").cast("float"))\
           .withColumn("user_id", f.col("user_id").cast("int"))

data.printSchema()
add_log( 'Filtre du '+DATE_START+ " au " + DATE_END)
add_log( str(data.count()) + ' lignes')
#granularité cible : user_session/product_id
#avec calcul colonne purcharsed/num_view_product
data_cible = data.withColumn(
                        'nb_purchased',
                        f.when(f.col('event_type') == 'purchase', 1).otherwise(0)
                  ) \
                 .withColumn(
                        'nb_views',
                        f.when(f.col('event_type') == 'view', 1).otherwise(0)
                    ) \
                 .groupBy("product_id","user_session","user_id","category_id","category_code","brand","price") \
                 .agg(
                    f.sum('nb_purchased').alias('nb_purchased'),
                    f.sum('nb_views').alias('num_views_product')
                 )\
                 .withColumn(
                        'purchased',
                         f.when(f.col('nb_purchased') > 0, f.lit(True)).otherwise(f.lit(False))
                  )

data_cible.show(5)
add_log( str(data.count()) + ' lignes dans data_cible')
data_cible.printSchema()

#granularité  : user_session
#avec calcul colonnes
data_session = data.withColumn(
                        'nb_views',
                        f.when(f.col('event_type') == 'view', 1).otherwise(0)
                    ) \
                 .groupBy("user_session","user_id") \
                 .agg(
                    f.min('event_time').alias('date_session_start'),
                    f.max('event_time').alias('date_session_end'),
                    f.sum('nb_views').alias('num_views_session')
                 )\
                 .withColumn( "hour_session_start", f.hour("date_session_start") ) \
                 .withColumn( "min_session_start", f.minute("date_session_start") ) \
                 .withColumn( "day_session_start", f.dayofweek("date_session_start") ) \
                 .withColumn( "duration",  (f.col("date_session_end") - f.col("date_session_start")).cast("int"))\
                 .withColumn("num_prev_sessions", f.row_number().over(Window.partitionBy("user_id").orderBy("date_session_start")))

#add_log( str(data.count()) + ' lignes dans data_session')

add_log( 'Fin calcul data_session :')
data_session.show(5)
data_session.printSchema()


#Jointure des tables précédements calculés
final_data = data_cible.alias("d").join(data_session.alias("session"), ( ( data_cible.user_session ==  data_session.user_session) & ( data_cible.user_id ==  data_session.user_id) ),"outer")\
                       .select("product_id","d.user_session","d.user_id" ,"category_id","category_code","brand","price","purchased"\
                               ,"num_views_product","num_views_session","hour_session_start","min_session_start","day_session_start","date_session_start"\
                               ,"duration","num_prev_sessions")\
                       .withColumn("num_prev_product_views", f.sum("d.num_views_product").over(Window().partitionBy("user_id","product_id").orderBy("session.date_session_start").rowsBetween(Window.unboundedPreceding, Window.currentRow) ))

final_data.show(5,truncate=False)
add_log( str(data.count()) + ' lignes dans final_data')

file_name=datetime.now().strftime("%y%m%d%H%M%S")+"_EcomData_"+DATE_START+"_"+DATE_END+".csv"

#Ecriture du fichier csv final
final_data.write.option("header",True) \
                .csv(DESTINATION+"/"+file_name)
add_log( 'Fichier out généré : '+DESTINATION+"/"+file_name )

