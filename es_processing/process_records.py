import logging
from dataclasses import dataclass, field
from typing import Dict
from uuid import uuid4

from decouple import config
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import RequestError
from elasticsearch.helpers import bulk


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

FULL_DAM_MAPPINGS = {
    "properties": {
        "object_url": {"type": "text", "analyzer": "keyword"},
        "object_uri": {"type": "text", "analyzer": "keyword"},
        "object_fid": {"type": "text", "analyzer": "keyword"},
        "object_datetime": {"type": "date"},
        "thumb": {
            "properties": {
                "l": {"type": "text", "analyzer": "keyword"},
                "m": {"type": "text", "analyzer": "keyword"},
                "s": {"type": "text", "analyzer": "keyword"}
            }
        },
        "tags": {"type": "keyword"},
        "key_value": {
            "type": "nested",
            "properties": {
                "key": {"type": "keyword"},
                "value": {"type": "keyword"}
            }
        }
    }
}

DAM_DOC_SAMPLES = [
    {
        "_index": "dam1",
        "_id": str(uuid4()),
        "_source": {
            "object_url": "https://example.com/object1",
            "object_uri": "object1",
            "object_fid": "object1",
            "object_datetime": "2021-08-01T12:00:00",
            "thumb": {
                "l": "https://example.com/thumb1_l",
                "m": "https://example.com/thumb1_m",
                "s": "https://example.com/thumb1_s"
            },
            "tags": ["tag1", "tag2", "tag3"],
            "key_value": [
                {"key": "key1", "value": "value1"},
                {"key": "key2", "value": "value2"}
            ]
        }
    },
    {
        "_index": "dam1",
        "_id": str(uuid4()),
        "_source": {
            "object_url": "https://example.com/object2",
            "object_uri": "object2",
            "object_fid": "object2",
            "object_datetime": "2021-08-02T12:00:00",
            "thumb": {
                "l": "https://example.com/thumb2_l",
                "m": "https://example.com/thumb2_m",
                "s": "https://example.com/thumb2_s"
            },
            "tags": ["tag1", "tag3", "tag4"],
            "key_value": [
                {"key": "key1", "value": "value1"},
                {"key": "key3", "value": "value3"}
            ]
        }
    },
    {
        "_index": "dam1",
        "_id": str(uuid4()),
        "_source": {
            "object_url": "https://example.com/object3",
            "object_uri": "object3",
            "object_fid": "object3",
            "object_datetime": "2021-08-03T12:00:00",
            "thumb": {
                "l": "https://example.com/thumb3_l",
                "m": "https://example.com/thumb3_m",
                "s": "https://example.com/thumb3_s"
            },
            "tags": ["tag2", "tag3", "tag4"],
            "key_value": [
                {"key": "key2", "value": "value2"},
                {"key": "key3", "value": "value3"}
            ]
        }

    },
    {
        "_index": "dam1",
        "_id": str(uuid4()),
        "_source": {
            "object_url": "https://example.com/object4",
            "object_uri": "object4",
            "object_fid": "object4",
            "object_datetime": "2021-08-04T12:00:00",
            "thumb": {
                "l": "https://example.com/thumb4_l",
                "m": "https://example.com/thumb4_m",
                "s": "https://example.com/thumb4_s"
            },
            "tags": ["tag1", "tag2", "tag4"],
            "key_value": [
                {"key": "key1", "value": "value1"},
                {"key": "key2", "value": "value2"}
            ]
        }
    }
]

queries = {
    "simple_query": {
        "query": {
            "match": {
                "tags": "tag1"
            }
        }
    },
    "by_oid": {
        "query": {
            "match": {
                "tags": "tag1"
            }
        }
    },
    "by_tag": {
        "query": {
            "match": {
                "tags": "tag1"
            }
        }
    },
    "by_key_value": {
        "query": {
            "match": {
                "tags": "tag1"
            }
        }
    },
}


@dataclass
class QUERIES:
    by_oid: Dict[str, str] = field(default_factory=lambda: queries["by_oid"])
    by_tag: Dict[str, str] = field(default_factory=lambda: queries["by_tag"])
    by_key_value: Dict[str, str] = field(default_factory=lambda: queries["by_tag"])
    simple_query: Dict[str, str] = field(default_factory=lambda: queries["simple_query"])


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

    def bulk_index_documents(self, index, documents):
        """Bulk index documents in the Elasticsearch index."""
        try:
            success, failed = bulk(client=self.es, actions=documents, index=index)
            log.info(f"Success: {success}, Failed: {failed}")
        except RequestError as e:
            log.error(f"Could not bulk index documents in the index: {index}")
            log.error(e)

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
    q = QUERIES()
    es_processor = ESProcessor()
    es_processor.verify_es_connection()
    es_processor.search_documents("dam1", q.simple_query)
