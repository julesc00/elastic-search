import asyncio
from elasticsearch import AsyncElasticsearch, Elasticsearch
import pandas as pd
from pprint import pprint

host = "http://localhost:9200"
es = AsyncElasticsearch(host)
es2 = Elasticsearch(host)


def show_info():
    print(es2.info().body)


def create_index(index_name, mappings):
    try:
        es2.indices.create(index=index_name, mappings=mappings)
        print("Index created successfully")
    except Exception as e:
        print(e)


def read_csv(csv_file):
    try:
        df = (
            pd.read_csv(csv_file)
            .dropna()
            .sample(5000, random_state=50)
            .reset_index()
        )
        print("Data read successfully")
        return df
    except Exception as e:
        print(e)


def store_data(index_name):
    try:
        df = read_csv("wiki_movie_plots_deduped.csv")
        for i, row in df.iterrows():
            doc = {
                "title": row["Title"],
                "ethnicity": row["Origin/Ethnicity"],
                "director": row["Director"],
                "cast": row["Cast"],
                "genre": row["Genre"],
                "plot": row["Plot"],
                "year": row["Release Year"],
                "wiki_page": row["Wiki Page"]
            }
            print(f"Storing document {i}")
            es2.index(index="movies", id=i, document=doc)
        print("Data stored successfully")
        es2.indices.refresh(index="movies")
        print(es2.count(index="movies", format="json"))
    except Exception as e:
        print(e)


def search_items():
    try:
        res = es2.search(
            index="movies",
            query={
                "bool": {
                    "must": {
                        "match_phrase": {
                            "cast": "jack nicholson",
                        }
                    },
                    "filter": {"bool": {"must_not": {"match_phrase": {"director": "roman polanski"}}}},
                },
            },
        )
        pprint(res.body)
    except Exception as e:
        print(e)


if __name__ == "__main__":
    mappings = {
        "properties": {
            "title": {"type": "text", "analyzer": "english"},
            "ethnicity": {"type": "text", "analyzer": "standard"},
            "director": {"type": "text", "analyzer": "standard"},
            "cast": {"type": "text", "analyzer": "standard"},
            "genre": {"type": "text", "analyzer": "standard"},
            "plot": {"type": "text", "analyzer": "english"},
            "year": {"type": "integer"},
            "wiki_page": {"type": "keyword"}
        }
    }

    show_info()
    # asyncio.run(main())
    # create_index("movies", mappings=mappings)
    # store_data("movies")
    search_items()
