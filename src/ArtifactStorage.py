from abc import ABC, abstractmethod
from typing import Set, Dict, Any

class ArtifactStorage(ABC):
    @abstractmethod
    def add_artifact(self, artifact_id: str, metadata: Dict[str, Any]):
        pass

    @abstractmethod
    def get_artifact(self, artifact_id: str) -> Dict[str, Any]:
        pass

    @abstractmethod
    def query_by_attribute(self, attribute: str, value: Any) -> Set[str]:
        """Return artifact IDs matching a single attribute"""
        pass

