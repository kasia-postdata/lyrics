import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

index_name = "songs"
mapping = {
    "mappings": {
        "properties": {
            "lyrics": {"type": "text"},
            "spotify_id": {"type": "text"},
            "vector": {"type": "dense_vector", "dims": 768}
        }
    }
}

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
es.indices.delete(index=index_name, ignore=[400, 404])
response = es.indices.create(
    index=index_name,
    body=mapping,
    ignore=400
)

print('response:', response)


def getQuotes():
    quotes = pd.read_parquet('lyrics_to_index.parquet', engine='pyarrow')

    for index, row in quotes.iterrows():
        print(row['spotify_id'], type(row['bert_embeddings']))

        yield {
            "_index": index_name,
            "lyrics": row['lyrics'].strip(),
            "spotify_id": row['spotify_id'],
            "vector": row['bert_embeddings']
        }


bulk(client=es, actions=getQuotes(), chunk_size=1000, request_timeout=120)
