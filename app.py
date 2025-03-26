# Point d’entrée de votre application 
# app.py
from db.mongo_connection import get_mongo_client, get_films_collection

def main():
    # 1. On initialise le client Mongo
    client = get_mongo_client()

    # 2. On récupère la collection "films"
    films_collection = get_films_collection(client)

    # 3. On teste en comptant les documents
    count_docs = films_collection.count_documents({})
    print(f"Il y a {count_docs} documents dans la collection 'films'.")

if __name__ == "__main__":
    main()
