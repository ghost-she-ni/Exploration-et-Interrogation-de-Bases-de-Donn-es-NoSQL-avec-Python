"""
Application Streamlit pour démontrer l'utilisation des requêtes MongoDB.

Ce fichier se connecte à la base via mongo_connection.py, 
puis appelle les fonctions définies dans mongo_queries.py.
"""

import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt

from db.mongo_connection import get_mongo_client, get_films_collection
from queries.mongo_queries import (
    year_with_most_releases,
    count_films_after_1999,
    average_votes_for_2007,
    films_per_year,
    distinct_genres,
    top_revenue_film,
    directors_with_more_than_5_movies,
    top_genre_by_average_revenue,
    top_3_movies_each_decade,
    longest_film_by_genre,
    create_view_high_metascore_and_revenue,
    runtime_revenue_correlation,
    average_runtime_by_decade
)

def main():
    st.title("Projet NoSQL - Requêtes sur la collection 'films'")

    # Connexion Mongo
    client = get_mongo_client()
    films_collection = get_films_collection(client)

    # Menu pour sélectionner la requête
    options = [
        "1) Année la plus prolifique",
        "2) Nombre de films après 1999",
        "3) Moyenne des Votes en 2007",
        "4) Nombre de films par année (pour histogramme)",
        "5) Genres distincts",
        "6) Film au plus gros revenu",
        "7) Réalisateurs > 5 films",
        "8) Genre au plus haut revenu moyen",
        "9) Top 3 films par décennie (Metascore)",
        "10) Film le plus long par genre",
        "11) Créer la vue high_metascore_and_revenue_view",
        "12) Corrélation durée / revenu",
        "13) Durée moyenne par décennie"
    ]
    choice = st.selectbox("Choisissez une requête :", options)

    if st.button("Exécuter"):
        if choice.startswith("1)"):
            year, count = year_with_most_releases(films_collection)
            if year:
                st.write(f"Année: {year}, nombre de films: {count}")
            else:
                st.write("Aucun résultat.")

        elif choice.startswith("2)"):
            nb = count_films_after_1999(films_collection)
            st.write(f"{nb} films après 1999")

        elif choice.startswith("3)"):
            avg_votes = average_votes_for_2007(films_collection)
            if avg_votes is not None:
                st.write(f"Moyenne des Votes en 2007: {avg_votes:.2f}")
            else:
                st.write("Aucun film trouvé pour 2007.")

    
        elif choice.startswith("4)"):
            data = films_per_year(films_collection)
            df = pd.DataFrame(data)
            df.rename(columns={"_id": "Year", "count": "Film Count"}, inplace=True)
            df["Year"] = pd.to_numeric(df["Year"], errors="coerce")
            df = df.dropna(subset=["Year"])
            df = df.sort_values("Year")
            st.bar_chart(data=df, x="Year", y="Film Count")
            

        elif choice.startswith("5)"):
            genres = distinct_genres(films_collection)
            st.write(f"Genres distincts ( {len(genres)} ) :")
            for g in genres:
                st.write("-", g)

        elif choice.startswith("6)"):
            film = top_revenue_film(films_collection)
            if film:
                st.write("Film avec le plus grand revenu (Millions) :")
                st.json(film)
            else:
                st.write("Aucun film trouvé.")

        elif choice.startswith("7)"):
            directors = directors_with_more_than_5_movies(films_collection)
            if directors:
                st.write("Réalisateurs ayant fait plus de 5 films :")
                for d in directors:
                    st.write(f"- {d['_id']}: {d['count']} films")
            else:
                st.write("Aucun réalisateur ne dépasse 5 films.")

        elif choice.startswith("8)"):
            best_genre = top_genre_by_average_revenue(films_collection)
            if best_genre:
                st.write(f"Genre le plus rentable en moyenne : {best_genre['_id']}")
                st.write(f"Revenu moyen : {best_genre['avgRevenue']:.2f} M$")
            else:
                st.write("Aucun résultat.")

        elif choice.startswith("9)"):
            results = top_3_movies_each_decade(films_collection)
            for decade_info in results:
                decade = decade_info["decade"]
                top3 = decade_info["top3Films"]
                st.write(f"### Décennie : {decade}")
                for film in top3:
                    st.write(f"- {film['title']} (year={film['year']}, Metascore={film['metascore']})")

        elif choice.startswith("10)"):
            results = longest_film_by_genre(films_collection)
            st.write("Film le plus long par genre :")
            for doc in results:
                st.write(
                    f"Genre: {doc['_id']} | Durée: {doc['maxRuntime']} min | "
                    f"Film: {doc['longestFilm']['title']} (year: {doc['longestFilm']['year']})"
                )

        elif choice.startswith("11)"):
            create_view_high_metascore_and_revenue(client)
            st.write("Vue 'high_metascore_and_revenue_view' créée avec succès (si elle n'existait pas).")

        elif choice.startswith("12)"):
            corr = runtime_revenue_correlation(films_collection)
            if corr is not None:
                st.write(f"Corrélation (Pearson) entre durée et revenu (Millions): {corr:.3f}")
            else:
                st.write("Pas assez de données pour calculer la corrélation.")

        elif choice.startswith("13)"):
            results = average_runtime_by_decade(films_collection)
            if not results:
                st.write("Aucune donnée disponible.")
            else:
                st.write("Durée moyenne (minutes) par décennie :")
                for doc in results:
                    st.write(f"- Décennie {doc['decade']} : {doc['avgRuntime']:.2f} minutes")

if __name__ == "__main__":
    main()

