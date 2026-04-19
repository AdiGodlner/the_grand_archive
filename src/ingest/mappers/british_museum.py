"""
British Museum Mapper
Handles the mapping of raw N-Quads from the British Museum's
CIDOC-CRM flavored RDF export into the Grand Archive's internal schema.
"""

class BM_MAPPER_ERROR(Exception):
    pass


BM_BASE = b"http://collection.britishmuseum.org"
LEN_BM_BASE = len(BM_BASE)
PREDICATES_PREFIXS = [


]



# b'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
# b'http://www.w3.org/2000/01/rdf-schema#comment'
# b'http://www.w3.org/2000/01/rdf-schema#label'
# b'http://www.w3.org/2004/02/skos/core#altLabel'
# b'http://www.w3.org/2004/02/skos/core#broader'
# b'http://www.w3.org/2004/02/skos/core#inScheme'
# b'http://www.w3.org/2004/02/skos/core#prefLabel'
# b'http://www.w3.org/2004/02/skos/core#related'
# b'http://www.w3.org/2004/02/skos/core#scopeNote'
# b'http://www.w3.org/ns/prov#generatedAtTime'"

def extract_id(subject_bytes):
    try:
        if subject_bytes.startswith(BM_BASE):
            # We keep the /id/object/YCA48 part
            path = subject_bytes[LEN_BM_BASE:].decode('utf-8')
            parts = path.split('/')
            parts[3] = "{ID}" # replace changing ID with generic for sets
            schema_pattern = "/".join(parts)
            return schema_pattern
        else:
            # Fallback for external URIs (like CIDOC or RDF types)
            schema_pattern = subject_bytes.decode('utf-8')
            return schema_pattern
    except Exception as e:
        print(e)
        schema_pattern = subject_bytes.decode('utf-8')
        return schema_pattern

C_LOWER = b'c'[0]
E_LOWER = b'e'[0]
P_LOWER = b'p'[0]
W_LOWER = b'w'[0]
BM_ONTOLOGY_PREFIX = 48
ERLANGEN_ONTOLOGY_PREFIX = 32
PURL_ONTOLOGY_PREFIX = 32
W3_ONTOLOGY_PREFIX = 18

def map_predicate(predicate_bytes):

    c = predicate_bytes[7]
    if c == C_LOWER:
        predicate_bytes = predicate_bytes[BM_ONTOLOGY_PREFIX:]
    elif c == E_LOWER:
        predicate_bytes = predicate_bytes[ERLANGEN_ONTOLOGY_PREFIX:]
    elif c == P_LOWER:
        predicate_bytes = predicate_bytes[PURL_ONTOLOGY_PREFIX:]
    elif c == W_LOWER:
        predicate_bytes = predicate_bytes[W3_ONTOLOGY_PREFIX:]
    else:
        raise BM_MAPPER_ERROR("British museum predicate of unknown type")

    return predicate_bytes

def transform_object(obj_tuple):
    """
    Processes the 'tagged union' object tuple.
    Decodes bytes and structures the data for JSON storage.
    """
    pass


def bm_mapper(quad):
    """
    The main orchestration function for British Museum quads.
    Takes a raw quad (s, p, o, g) and returns a (id, predicate, payload)
    ready for the QuadStore upsert.
    """
    subject, predicate, obj, graph = quad
    subject_id = extract_id(subject)

    return subject_id, predicate, obj, graph


_predicate_file = "/home/adi/Desktop/keep/code/git/repos/the_grand_archive/maybe_useful/bm_eval/pred_out.txt"

if __name__ == '__main__':

    with open(_predicate_file, "rb") as f:
        for line in f:
            try:
               pred_bytes = map_predicate(line)
               print(pred_bytes[:-1].decode('utf-8'))
            except Exception as e:
                pass
