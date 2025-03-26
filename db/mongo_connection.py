"""
Ce module gère la connexion à la base MongoDB sur Atlas .
On y lit l'URI de connexion depuis config.py pour éviter
d'exposer les identifiants dans le code versionné.
"""

from pymongo import MongoClient

# On importe la constante MONGO_URI qui est définie dans config.py
# Assurez-vous que config.py est à la racine du projet et dans le .gitignore.
try:
    from config import MONGO_URI
except ImportError:
    raise Exception("Impossible de trouver 'config.py'. Assurez-vous qu'il est bien dans la racine du projet et ignoré par Git.")

def get_mongo_client():
    """
    Initialise et renvoie un client MongoDB en utilisant l'URI stocké
    dans config.py (MONGO_URI).

    :return: Un objet pymongo.MongoClient connecté à l'instance MongoDB spécifiée.
    """
    client = MongoClient(MONGO_URI)
    return client

def get_films_collection(client):
    """
    Retourne la collection 'films' de la base 'entertainment'.

    :param client: Un objet pymongo.MongoClient déjà connecté à MongoDB.
    :return: Une Collection MongoDB pointant sur 'entertainment.films'.
    """
    db = client["entertainment"]
    return db["films"]
