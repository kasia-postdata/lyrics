from elasticsearch import Elasticsearch
import json
import pandas as pd

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

all_indices = es.indices.stats( human=True)['indices']
print(all_indices)

def findRelevantHits(inQuiry, inQuiry_vector):
    queries = {
        'bert': {
            "script_score": {
                "query": {
                    "match_all": {}
                },
                "script": {
                    "source": "cosineSimilarity(params.inQuiry_vector, doc['vector']) + 1.0",
                    "params": {
                        "inQuiry_vector": inQuiry_vector
                    }
                }
            }
        },
        'mlt': {
            "more_like_this": {
                "fields": ["lyrics"],
                "like": inQuiry,
                "min_term_freq": 1,
                "max_query_terms": 50,
                "min_doc_freq": 1
            }
        }
    }

    result = {'bert': [], 'mlt': []}

    for metric, query in queries.items():
        body = {"query": query, "size": 3, "_source": ["lyrics", "spotify_id"]}
        response = es.search(index='songs', body=body, request_timeout=120)
        result[metric] = [a['_source'] for a in response['hits']['hits']]
    return result

queries = pd.read_csv('user_queries_emb.csv', sep='\t', names=['query','bert_embedding'])
queries["bert_embedding"] = queries["bert_embedding"].apply(lambda s: [float(x.strip(' []')) for x in s.split(',')])


with open("output", 'w') as outfile1:
    for index, row in queries.iterrows():
        results = {}
        relevant_songs = findRelevantHits(row['query'].strip().lower(), row['bert_embedding'])
        results[row['query'].strip()] = relevant_songs
        print(json.dumps(results, ensure_ascii=False), file=outfile1)



