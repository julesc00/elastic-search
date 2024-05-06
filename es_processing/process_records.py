import logging
import os
from dataclasses import dataclass
from time import sleep
from uuid import uuid4

from decouple import config
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ES App")


@dataclass
class ESCredentials:
    host: str
    port: str
    user: str
    password: str


credentials = ESCredentials(
    host=config("HOST"),
    port=config("PORT"),
    user=config("ELASTIC_USER"),
    password=config("PASSWORD"),
)


class ESProcessor:
    def __init__(self):
        self.host = credentials.host
        self.port = credentials.port
        self.url = f"{self.host}:{self.port}"
        self.user = credentials.user
        self.password = credentials.password
        self.es = Elasticsearch(hosts=self.url, basic_auth=(self.user, self.password), verify_certs=False)

    def verify_es_connection(self):
        """Verify the connection to the Elasticsearch server."""
        if self.es.ping():
            log.info("Connected to the Elasticsearch server.")
        else:
            log.error("Could not connect to the Elasticsearch server.")
            raise ConnectionError("Could not connect to the Elasticsearch server.")

    def create_index(self, index, mappings):
        """Create an Elasticsearch index with the specified mappings."""
        try:
            self.es.indices.create(index=index, body={"mappings": mappings})
            log.info(f"Index created: {index}")
        except RequestError as e:
            log.error(f"Error creating index: {e}")
            raise e

    def index_document(self, index, document):
        """Index a document in the specified index."""
        try:
            self.es.index(index=index, id=str(uuid4()), body=document)
            log.info("Document indexed successfully.")
        except RequestError as e:
            log.error(f"Error indexing document: {e}")
            raise e

    def update_document(self, index, doc_id, document):
        """Update a document in the specified index."""
        try:
            self.es.update(index=index, id=doc_id, body={"doc": document})
            log.info("Document updated successfully.")
        except RequestError as e:
            log.error(f"Error updating document: {e}")
            raise e

    def delete_document(self, index, doc_id):
        """Delete a document from the specified index."""
        try:
            self.es.delete(index=index, id=doc_id)
            log.info("Document deleted successfully.")
        except RequestError as e:
            log.error(f"Error deleting document: {e}")
            raise e

    def search_documents(self, index, query):
        """Search for documents in the specified index."""
        try:
            res = self.es.search(index=index, body=query)
            log.info(f"Search results: {res}")
            return res
        except RequestError as e:
            log.error(f"Error searching documents: {e}")
            raise e


if __name__ == "__main__":
    es_processor = ESProcessor()
    es_processor.verify_es_connection()
