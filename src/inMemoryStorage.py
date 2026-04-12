from typing import Set, Dict, Any
import ArtifactStorage

class InMemoryArtifactStorage(ArtifactStorage):

    def __init__(self):
        self.artifacts: Dict[str, Dict[str, Any]] = {}
        self.reverse_index: Dict[str, Dict[Any, Set[str]]] = {}

    def add_artifact(self, artifact_id: str, metadata: Dict[str, Any]):

        # TODO verify signature before adding to database
        #  should it be here but in the node functionality ?

        self.artifacts[artifact_id] = metadata

        for attr, value in metadata.items():

            # TODO this currently assumes a flat structure of tags
            #  look at real data from APIs to see if this needs changing
            if attr not in self.reverse_index:
                self.reverse_index[attr] = {}

            if value not in self.reverse_index[attr]:
                self.reverse_index[attr][value] = set()

            self.reverse_index[attr][value].add(artifact_id)

    def get_artifact(self, artifact_id: str) -> Dict[str, Any]:
        return self.artifacts.get(artifact_id, {})

    def query_by_attribute(self, attribute: str, value: Any) -> Set[str]:
        return self.reverse_index.get(attribute, {}).get(value, set())
