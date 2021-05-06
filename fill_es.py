from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

index_name = "songs"

es = Elasticsearch([{'host':'localhost','port':9200}])
es.indices.delete(index=index_name, ignore=[400, 404])

def getQuotes():
    f = open('./lyrics_to_index_6lines', 'r')
    for line in f:

        song_id, quote, vector = line.split('\t', 2)

        yield {
            "_index": index_name,
            "quote" : quote.strip(),
            "song_id": song_id,
            "vector" : vector
         }

bulk(client=es, actions = getQuotes(), chunk_size=1000, request_timeout = 120)
