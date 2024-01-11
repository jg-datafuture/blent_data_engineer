# Job Spark d'Extraction de Données ECOM

## Aperçu

Ce référentiel contient un job Spark écrit en Scala ou PySpark pour extraire des données brutes et générer un jeu de données d'apprentissage pour les data scientists, à exécuter sur un cluster Hadoop. Le processus implique la spécification de détails de destination et de plages de dates pour extraire des événements pertinents. Les données proviennent d'un enregistrement historique sur environ 7 mois, réparti sur plusieurs fichiers et stocké dans Google Cloud Storage (GCS).

## Source de Données

### Description

La source de données comprend un historique d'événements enregistrés sur près de 7 mois, stocké dans plusieurs fichiers (un par mois). 
Les données sont stockées dans GCS :
gs://projets_blent/ecom/blent-learning-user-ressources.s3.eu-west-3.amazonaws.com/projects/9c15cb
Les données ont été transférées vers GCS à l'aide du fichier TSV `ecom_proj.tsv`.

## Paramètres du Job Spark

Les data scientists peuvent lancer le job Spark en spécifiant les paramètres suivants :

- **DESTINATION** : La destination où la table de sortie (au format CSV) sera enregistrée. L'emplacement par défaut est `"gs://projets_blent/ecom/out/"`.

- **DATE_START** : La date de début à partir de laquelle les événements sont récupérés (format : AAAA-MM-JJ).

- **DATE_END** : La date de fin pour la récupération des événements (format : AAAA-MM-JJ).

## Description des Données en Sortie

### Granularité

Les données en sortie ont une granularité de `session_utilisateurs` / `article`.

### Champs

| Champ                | Type données | Description                                           |
|----------------------|--------------|-------------------------------------------------------|
| user_session         | String       |                                                       |
| product_id           | BigInt       |                                                       |
| user_id              | BigInt       |                                                       |
| category_id          | BigInt       | Un identifiant unique associé à la catégorie du produit |
| category_code        | String       | Un code associé à la catégorie du produit              |
| brand                | String       | Marque du produit                                      |
| price                | Float        |                                                       |
| purchased           | Boolean      | Indique si l'article a été acheté                      |
| num_views_product    | Int          | Le nombre de fois où l'article a été vue               |
| num_views_session    | Int          | Le nombre d'articles vus dans la session              |
| hour_session_start   | Int          | L'heure de la semaine où la session a démarré         |
| min_session_start    | Int          | La minute de la semaine où la session a démarré       |
| day_session_start    | String       | Jour de la semaine où la session a démarré             |
| duration             | Int          | Durée de la session                                    |
| num_prev_sessions    | Int          | Le nombre de sessions précédentes                      |
| num_prev_product_views| Int          | Le nombre de fois où l'article a été déjà vu dans des sessions précédentes |


## Utilisation

Pour exécuter le job Spark, utilisez le script Python `ECOM_script` avec les paramètres spécifiés. 
De plus, un notebook Jupyter (`ECOM_script.ipynb`) est fourni à titre de référence.
