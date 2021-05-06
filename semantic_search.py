from elasticsearch import Elasticsearch
import json

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

all_indices = es.indices.stats( human=True)['indices']
print(all_indices)

def findRelevantHits(inQuiry):
    inQuiry_vector = bc.encode([inQuiry])[0].tolist()
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
                "fields": ["quote"],
                "like": inQuiry,
                "min_term_freq": 1,
                "max_query_terms": 50,
                "min_doc_freq": 1
            }
        }
    }

    result = {'bert': [], 'mlt': []}

    for metric, query in queries.items():
        body = {"query": query, "size": 3, "_source": ["quote", "song_id"]}
        response = es.search(index='quotes', body=body, request_timeout=120)
        result[metric] = [a['_source'] for a in response['hits']['hits']]
    return result


with open("output", 'w') as outfile1:
    with open('user_queries.txt') as f:
        for line in f:
            results = {}
            relevant_songs = findRelevantHits(line.strip().lower())
            results[line.strip()] = relevant_songs
            print(json.dumps(results, ensure_ascii=False), file=outfile1)



