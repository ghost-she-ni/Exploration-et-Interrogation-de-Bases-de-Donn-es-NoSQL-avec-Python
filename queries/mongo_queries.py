"""
Ce module contient des fonctions de requêtes MongoDB pour la collection 'films'.
Elles prennent en argument la collection (films_collection) et renvoient
le ou les résultats des requêtes.
"""

from pymongo.collection import Collection
from typing import Any, List, Dict, Tuple, Optional
import pandas as pd

def year_with_most_releases(films_collection: Collection) -> Tuple[Any, int]:
    """
    1. Afficher l'année où le plus grand nombre de films sont sortis.

    :param films_collection: Collection MongoDB "films"
    :return: (year, count) où 'year' est l'année et 'count' le nombre de films.
             (None, 0) si aucun résultat.
    """
    pipeline = [
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 1}
    ]
    result = list(films_collection.aggregate(pipeline))
    if result:
        return result[0]["_id"], result[0]["count"]
    else:
        return None, 0

def count_films_after_1999(films_collection: Collection) -> int:
    """
    2. Quel est le nombre de films sortis après l’année 1999 ?

    :param films_collection: Collection MongoDB "films"
    :return: Nombre de films où 'year' > 1999.
    """
    return films_collection.count_documents({"year": {"$gt": 1999}})

def average_votes_for_2007(films_collection: Collection) -> float:
    """
    3. Quelle est la moyenne des VOTES des films sortis en 2007 ?
       (On interprète la 'moyenne des votes' comme la moyenne de Votes.)

    :param films_collection: Collection MongoDB "films"
    :return: Valeur moyenne (float) ou None si aucun film de 2007
    """
    pipeline = [
        {"$match": {"year": 2007}},
        {"$group": {"_id": None, "avgVotes": {"$avg": "$Votes"}}}
    ]
    result = list(films_collection.aggregate(pipeline))
    return result[0]["avgVotes"] if result else None

def films_per_year(films_collection: Collection) -> List[Dict[str, Any]]:
    """
    4. Récupère la liste du nombre de films par année,
       afin d'afficher ensuite un histogramme.

    :param films_collection: Collection MongoDB "films"
    :return: Liste d'objets { "_id": year, "count": nbFilms }
    """
    pipeline = [
        {"$group": {"_id": "$year", "count": {"$sum": 1}}},
        {"$sort": {"_id": 1}}
    ]
    return list(films_collection.aggregate(pipeline))

def distinct_genres(films_collection: Collection) -> List[str]:
    """
    5. Quelles sont les genres de films disponibles dans la base ?
       Ici, 'genre' est une chaîne du style "Crime,Drama,Mystery".
       Pour lister distinctement chaque genre, il faut agréger avec $split.

    :param films_collection: Collection MongoDB "films"
    :return: Liste des genres distincts (sans doublons).
    """
    pipeline = [
        {
            # On crée un champ 'genreArray' qui est un tableau
            "$addFields": {
                "genreArray": {
                    "$split": ["$genre", ","]
                }
            }
        },
        {
            # On éclate ce tableau
            "$unwind": "$genreArray"
        },
        {
            # On regroupe par genre unique
            "$group": {
                "_id": "$genreArray"
            }
        }
    ]
    results = list(films_collection.aggregate(pipeline))
    # results est une liste de documents [{"_id": "Crime"}, {"_id": "Drama"}, ...]
    # On renvoie une liste de strings
    return [doc["_id"].strip() for doc in results if doc["_id"]]

def top_revenue_film(films_collection: Collection) -> Optional[Dict]:
    """
    6. Quel est le film qui a généré le plus de revenu ?
       (Champ 'Revenue (Millions)' pour le revenu.)

    :param films_collection: Collection MongoDB "films"
    :return: Le document complet du film (dict) ou None si la collection est vide.
    """
    pipeline = [
        # 1. Convertir la valeur de "Revenue (Millions)" en nombre
        {
           "$addFields": {
             "parsedRevenue": {
               "$convert": {
                 "input": "$Revenue (Millions)",
                 "to": "double",
                 "onError": None,   # si la conversion échoue
                 "onNull": None     # si la valeur est null ou n’existe pas
               }
             }
           }
         },
         # 2. Filtrer pour ne garder que les documents où parsedRevenue est valable
         {
           "$match": {
             "parsedRevenue": { "$ne": None }  # exclude documents où la conversion n’a pas abouti
          }
         },
         # 3. Trier par la valeur numérique du revenu
         {
           "$sort": {
             "parsedRevenue": -1
           }
         },
         # 4. Conserver seulement le premier document (le film ayant généré le plus de revenus)
         {
           "$limit": 1
         },
         # 5. Projeter le résultat final, par exemple, titre et revenu
         {
           "$project": {
             "_id": 0,
             "title": 1,
             "originalRevenue": "$Revenue (Millions)",
             "numericRevenue": "$parsedRevenue"
           }
         }
       ]
    result = list(films_collection.aggregate(pipeline))
    return result[0] if result else None

def directors_with_more_than_5_movies(films_collection: Collection) -> List[Dict[str, Any]]:
    """
    7. Quels sont les réalisateurs ayant réalisé plus de 5 films ?

    :param films_collection: Collection MongoDB "films"
    :return: Liste de documents { "_id": DirectorName, "count": NombreDeFilms }
    """
    pipeline = [
        {"$group": {"_id": "$Director", "count": {"$sum": 1}}},
        {"$match": {"count": {"$gt": 5}}},
        {"$sort": {"count": -1}}
    ]
    return list(films_collection.aggregate(pipeline))

def top_genre_by_average_revenue(films_collection: Collection) -> Dict[str, Any]:
    """
    8. Quel est le genre de film qui rapporte en moyenne le plus de revenus ?
       On considère 'Revenue (Millions)' comme le revenu.
       On doit séparer 'genre' en plusieurs genres avant d'agréger.

    :param films_collection: Collection MongoDB "films"
    :return: Le genre avec son revenu moyen max, par ex:
             { "_id": "Action", "avgRevenue": 123.45 }
    """
    pipeline = [
        # 1) Convertir 'genre' en tableau
        {"$addFields": {
            "genreArray": {"$split": ["$genre", ","]}
        }},
        # 2) Déplier ce tableau
        {"$unwind": "$genreArray"},
        # 3) Regrouper par genre pour calculer la moyenne de 'Revenue (Millions)'
        {"$group": {
            "_id": {"$trim": {"input": "$genreArray"}},
            "avgRevenue": {"$avg": "$Revenue (Millions)"}
        }},
        # 4) Trier par avgRevenue décroissant
        {"$sort": {"avgRevenue": -1}},
        {"$limit": 1}
    ]
    result = list(films_collection.aggregate(pipeline))
    return result[0] if result else {}

def top_3_movies_each_decade(films_collection: Collection) -> List[Dict[str, Any]]:
    """
    9. Quels sont les 3 films les mieux notés (rating) pour chaque décennie ?
       Problème: le champ 'rating' est souvent une chaîne ('unrated', 'PG-13', etc.).
       => On peut interpréter "les mieux notés" comme "les plus hauts Votes"
          ou "les plus hauts Metascore". 
       Ici, on suppose qu'on utilise 'Metascore' (note critique) pour le "rating".

    :param films_collection: Collection MongoDB "films"
    :return: Liste de documents { "_id": "1990s", "topMovies": [ {...}, {...}, {...} ] }
    """

    pipeline = [
        # 1) Convertir Metascore en double
        {
            "$addFields": {
                "numericMetascore": {
                    "$convert": {
                        "input": "$Metascore",
                        "to": "double",
                        "onError": None,  # en cas d'échec de conversion
                        "onNull": None    # si champ inexistant ou null
                    }
                }
            }
        },
        # 2) Ignorer ceux où la conversion a échoué
        {
            "$match": {
                "numericMetascore": {"$ne": None}
            }
        },
        # 3) Calcul de la décennie : decade = year - (year % 10)
        {
            "$addFields": {
                "decade": {
                    "$subtract": [
                        "$year",
                        {"$mod": ["$year", 10]}
                    ]
                }
            }
        },
        # 4) Trier par Metascore décroissant
        {
            "$sort": {"numericMetascore": -1}
        },
        # 5) Regrouper par décennie, accumulateur de tous les films
        {
            "$group": {
                "_id": "$decade",
                "filmsByDecade": {
                    "$push": {
                        "title": "$title",
                        "metascore": "$numericMetascore",
                        "year": "$year"
                    }
                }
            }
        },
        # 6) Ne garder que les 3 premiers films de chaque décennie
        {
            "$project": {
                "_id": 0,
                "decade": "$_id",
                "top3Films": {"$slice": ["$filmsByDecade", 3]}
            }
        },
        # 7) Trier par décennie croissante
        {
            "$sort": {
                "decade": 1
            }
        }
    ]

    results = list(films_collection.aggregate(pipeline))
    return results

def longest_film_by_genre(films_collection: Collection) -> List[Dict[str, Any]]:
    """
    10. Quel est le film le plus long (Runtime) par genre ?
        Le champ s'appelle 'Runtime (Minutes)'.
        On sépare 'genre' pour chaque genre, puis on trie par runtime descendant.

    :param films_collection: Collection MongoDB "films"
    :return: Pour chaque genre, un document : 
             { "_id": "Action", "longestFilm": { ... }, "maxRuntime": 180 }
    """
    pipeline = [
        {"$addFields": {
            "genreArray": {"$split": ["$genre", ","]}
        }},
        {"$unwind": "$genreArray"},
        {"$set": {
            "genreArray": {"$trim": {"input": "$genreArray"}}
        }},
        {"$sort": {"Runtime (Minutes)": -1}},
        {
            "$group": {
                "_id": "$genreArray",
                "maxRuntime": {"$first": "$Runtime (Minutes)"},
                "longestFilm": {"$first": "$$ROOT"}
            }
        },
        {"$sort": {"maxRuntime": -1}}
    ]
    return list(films_collection.aggregate(pipeline))

def create_view_high_metascore_and_revenue(client):
    """
    11. Créer une vue MongoDB affichant uniquement les films
        ayant une note > 80 (Metascore)
        et généré plus de 50 millions de dollars (Revenue (Millions)).

    :param client: MongoClient pour exécuter la commande 'createView'.
    """
    db = client["entertainment"]
    db.command({
        "create": "high_metascore_and_revenue_view",
        "viewOn": "films",
        "pipeline": [
            {
                "$match": {
                    "Metascore": {"$gt": 80},
                    "Revenue (Millions)": {"$gt": 50.0}
                }
            }
        ]
    })

def runtime_revenue_correlation(films_collection: Collection) -> float:
    """
    12. Calculer la corrélation entre la durée des films (Runtime (Minutes))
        et leur revenu (Revenue (Millions)).

    :param films_collection: Collection MongoDB "films"
    :return: Coefficient de corrélation (Pearson) entre ces deux champs,
             ou None si pas de données suffisantes.
    """
    data = list(films_collection.find(
        {
            "Runtime (Minutes)": {"$exists": True, "$type": "number"},
            "Revenue (Millions)": {"$exists": True, "$type": "number"}
        },
        {
            "_id": 0,
            "Runtime (Minutes)": 1,
            "Revenue (Millions)": 1
        }
    ))
    if not data:
        return None
    df = pd.DataFrame(data)
    # On renomme pour simplifier
    df.rename(columns={
        "Runtime (Minutes)": "runtime",
        "Revenue (Millions)": "revenue"
    }, inplace=True)
    return df["runtime"].corr(df["revenue"])

def average_runtime_by_decade(films_collection: Collection) -> List[Dict[str, Any]]:
    """
    13. Y a-t-il une évolution de la durée moyenne des films par décennie ?

    :param films_collection: Collection MongoDB "films"
    :return: Liste de docs [ {"_id": "1990s", "avgRuntime": XX}, ... ] triés par décennie
    """
    pipeline = [
        # 1) Convertir Runtime (Minutes) en double
        {
            "$addFields": {
                "numericRuntime": {
                    "$convert": {
                        "input": "$Runtime (Minutes)",
                        "to": "double",
                        "onError": None,  # En cas d'échec de conversion
                        "onNull": None    # Si la valeur est null ou inexistante
                    }
                }
            }
        },
        # 2) Exclure les documents dont la conversion a échoué
        {
            "$match": {
                "numericRuntime": {"$ne": None}
            }
        },
        # 3) Calculer la décennie : decade = year - (year % 10)
        {
            "$addFields": {
                "decade": {
                    "$subtract": [
                        "$year",
                        {"$mod": ["$year", 10]}
                    ]
                }
            }
        },
        # 4) Grouper par 'decade' et calculer la durée moyenne
        {
            "$group": {
                "_id": "$decade",
                "avgRuntime": {"$avg": "$numericRuntime"}
            }
        },
        # 5) Projeter le résultat final
        {
            "$project": {
                "_id": 0,
                "decade": "$_id",
                "avgRuntime": 1
            }
        },
        # 6) Trier par décennie croissante
        {
            "$sort": {"decade": 1}
        }
    ]

    return list(films_collection.aggregate(pipeline))