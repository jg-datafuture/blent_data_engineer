# Job Spark d'Extraction de Données ECOM

## Contexte
Une entreprise ECommerce collecte de nombreuses informations sur ses utilisateurs lors de leurs visites et de leurs achats sur son site.
Afin d'inciter les utilisateurs hésitant à finaliser leur parcours d'achat, elle cherche à proposer des coupons de réduction à ces utilisateurs.
Les Data Scientists vont travailler sur un algorithme qui vise à construire un modèle prédictif qui va déterminer quels sont les utilisateurs susceptibles de finaliser leur parcours d'achat.
Afin de calibrer cet algorithme prédictif (Machine Learning), les Data Scientists ont besoin d'un historique de plusieurs jours d'événements qui peuvent s'étaler sur plusieurs semaines.
Ce référentiel contient un job Spark (écrit en PySpark) pour extraire des données brutes et générer un jeu de données d'apprentissage pour des data scientists, à exécuter sur un cluster Hadoop. 

## Source de Données

### Description

La source de données comprend un historique d'événements enregistrés sur près de 7 mois, stocké dans plusieurs fichiers (un par mois). 
Les données ont été transférées vers Google Cloud Storage à l'aide du fichier TSV : `ecom_proj.tsv`.
Lien GCS source par défaut : `gs://projets_blent/ecom/blent-learning-user-ressources.s3.eu-west-3.amazonaws.com/projects/9c15cb`

## Description des Données en Sortie

### Granularité

Les données en sortie ont une granularité : `session_utilisateurs` / `article`.

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
| duration             | BigInt        | Durée de la session en secondes                                   |
| num_prev_sessions    | Int          | Le nombre de sessions précédentes                      |
| num_prev_product_views| Int          | Le nombre de fois où l'article a été déjà vu dans des sessions précédentes |


## Utilisation

### Prérequis
- Ajouter le fichier gsc_key.txt contenant les informations de connexion à google cloud storage : par défaut dans "/home/admin/gsc_key.txt"


### Paramètres du Job Spark

Les data scientists peuvent lancer le job Spark en spécifiant les paramètres suivants :

Requis :
- **DATE_START** : La date de début à partir de laquelle les événements sont récupérés (format : AAAA-MM-JJ)

- **DATE_END** : La date de fin pour la récupération des événements (format : AAAA-MM-JJ)

Optionnel :
- **DESTINATION** : La destination où la table de sortie (au format CSV) sera enregistrée. L'emplacement par défaut est `"gs://projets_blent/ecom/out/"`

- **GCS_KEY_FILE_PATH** : Chemin vers le fichier de connexionGCS ( default="/home/admin/gsc_key.txt")

- **GCS_SOURCE** : Chemin GCS des données sources( default="gs://projets_blent/ecom/blent-learning-user-ressources.s3.eu-west-3.amazonaws.com/projects/9c15cb")

### Execution
Pour exécuter le job Spark ,utilisez le script Python `ECOM_extract_script.py` en spécifiant les paramètres ci dessus. 

Example de commande d'execution :
`python3 ECOM_extract_script.py --DATE_START=2020-03-20 --DATE_END=2020-04-07`
