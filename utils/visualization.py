import matplotlib.pyplot as plt

def plot_films_per_year(data):
    """
    Trace un histogramme du nombre de films par année.

    :param data: une liste de dicts ex: [{'_id': 1995, 'count': 12}, ...]
    :return: l'objet figure Matplotlib
    """
    # Extraire les années (X) et les counts (Y)
    years = [d["_id"] for d in data]
    counts = [d["count"] for d in data]

    # Créer la figure
    fig, ax = plt.subplots(figsize=(8, 4))  # vous pouvez ajuster la taille

    # Tracer un bar chart
    ax.bar(years, counts)

    # Labels et titre
    ax.set_xlabel("Année")
    ax.set_ylabel("Nombre de films")
    ax.set_title("Histogramme du nombre de films par année")

    # Ajuster éventuellement les graduations en x (si nombreuses années)
    plt.xticks(rotation=45)

    # Retourner la figure pour pouvoir l'afficher dans Streamlit
    return fig
