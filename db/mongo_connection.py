# Fonctions de connexion à MongoDB
from pymongo import MongoClient
import sys
import os

try:
    from config import MONGO_URI
except ImportError:
    raise Exception("Impossible de trouver 'config.py'. Assurez-vous qu'il est bien dans la racine du projet et ignoré par Git.")

def get_mongo_client():
    """
    Initialise et renvoie un client MongoDB en utilisant l'URI stocké
    dans config.py (MONGO_URI).
    """
    client = MongoClient(MONGO_URI)
    return client

def get_films_collection(client):
    """
    Retourne la collection 'films' de la base 'entertainment'.
    """
    db = client["entertainment"]
    return db["films"]
