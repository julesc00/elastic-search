import logging
from time import sleep
from uuid import uuid4

from decouple import config
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError
from elasticsearch.helpers import bulk


logging.basicConfig(level=logging.INFO)
log = logging.getLogger("Elasticsearch App")

HOST = config("HOST")
PORT = config("PORT")
URL = f"{HOST}:{PORT}"
USER = config("ELASTIC_USER")
PASSWORD = config("PASSWORD")
es_client = Elasticsearch(hosts=URL, basic_auth=(USER, PASSWORD), verify_certs=False)


MAPPINGS = {
    "properties": {
        "title": {"type": "text", "analyzer": "english"},
        "description": {"type": "text", "analyzer": "english"}
    }
}

DOCUMENT = {
    "title": "Python Programming",
    "description": "Learn Python programming from scratch."
}

JS_DOCUMENT = {
    "title": "JavaScript Programming",
    "description": "Learn JavaScript programming from scratch."
}

UPDATED_DOCUMENT = {
    "title": "Python Programming 7",
    "description": "Learn Python programming from scratch. Also, learn Django to build web apps."
}

DOCUMENTS = [
    {
        "_index": "courses",
        "_id": str(uuid4()),
        "_source": {
            "title": "Python Programming",
            "description": "Learn Python programming from scratch."
        }
    },
    {
        "_index": "courses",
        "_id": str(uuid4()),
        "_source": {
            "title": "JavaScript Programming",
            "description": "Learn JavaScript programming from scratch."
        }
    },
    {
        "_index": "courses",
        "_id": str(uuid4()),
        "_source": {
            "title": "Java Programming",
            "description": "Learn Java programming from scratch."
        }
    }
]


def verify_es_connection():
    """Verify the connection to the Elasticsearch server."""
    if es_client.ping():
        log.info("Connected to the Elasticsearch server.")
    else:
        log.error("Could not connect to the Elasticsearch server.")
        raise ConnectionError("Could not connect to the Elasticsearch server.")


def create_index(index, mappings):
    """Create an Elasticsearch index with the specified mappings."""
    try:
        es_client.indices.create(index=index, body={"mappings": mappings})
        log.info(f"Index created: {index}")
    except RequestError as e:
        log.error(f"Could not create the index: {index}")
        log.error(e)


def create_document(index, idx_id, document):
    """Create a document in the Elasticsearch index."""
    try:
        es_client.index(index=index, id=idx_id, document=document)
        log.info(f"Document created in the index: {index}, id: {idx_id}")
    except RequestError as e:
        log.error(f"Could not create the document in the index: {index}")
        log.error(e)


def update_document(index, idx_id, document):
    """Update a document in the Elasticsearch index."""
    try:
        res = es_client.update(index=index, id=idx_id, body={"doc": document})
        log.info(res)
        log.info(f"Document updated in the index: {index}, id: {idx_id}")
    except RequestError as e:
        log.error(f"Could not update the document in the index: {index}")
        log.error(e)


def delete_document(index, idx_id):
    """Delete a document from the Elasticsearch index."""
    try:
        es_client.delete(index=index, id=idx_id)
        log.info(f"Document deleted from the index: {index}, id: {idx_id}")
    except RequestError as e:
        log.error(f"Could not delete the document from the index: {index}")
        log.error(e)


def search_documents(index, query):
    """Search for documents in the specified index."""
    try:
        res = es_client.search(index=index, body=query)
        log.info(f"Search results: {res}")
        return res
    except RequestError as e:
        log.error(f"Error searching documents: {e}")
        raise e


if __name__ == "__main__":
    verify_es_connection()
    # create_document(index="courses", idx_id=str(uuid4()), document=JS_DOCUMENT)
    search_documents(index="courses", query={"query": {"match": {"title": "Python"}}})
    # update_document(index="courses", idx_id="2370259f-b5e5-4142-b772-22a6d5557011", document=UPDATED_DOCUMENT)
    # delete_document(index="courses", idx_id="b75177fc-5251-430d-9d97-bcd0181681d3")
    success, failed = bulk(client=es_client, actions=DOCUMENTS, index="courses")
    print(f"Success: {success}, Failed: {failed}")
