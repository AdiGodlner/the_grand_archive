import json
import os
from pathlib import Path
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding


class SignaturesManager:

    def __init__(self):
        self.trusted_publishers = []
        self.private_key = None

        self.load_trusted_publishers()

    def load_trusted_publishers(self):
        trusted_publishers_path = os.path.abspath("./trusted_publishers.json")

        with open(trusted_publishers_path , "r", encoding="utf-8") as f:
            data = json.load(f)

        self.trusted_publishers = data["trusted_publishers"]

    def load_private_key(self, path, password: str = None):
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Private key file does not exist: {path}")

        with open(path, "rb") as f:
            data = f.read()

        self.private_key = serialization.load_pem_private_key(
            data,
            password=password.encode() if password else None
        )

        return self.private_key

    def verify_artifact_signature(self, artifact):
        publisher_id = artifact["publisher_id"]

    def get_publishers_public_key(self, publisher_id):

        for publisher in self.trusted_publishers:
            if publisher["publisher_id"]:
                pass


def create_key_pair(output_folder, password: str = None):

    output_folder = Path(output_folder)
    output_folder.mkdir(parents=True, exist_ok=True)

    private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048
    )

    if password:
        encryption = serialization.BestAvailableEncryption(password.encode())
    else:
        encryption = serialization.NoEncryption()

    # Save private key
    private_path = output_folder / "private_key.pem"
    with open(private_path, "wb") as f:
        f.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=encryption
            )
        )

    # Save public key
    public_key = private_key.public_key()
    public_path = output_folder / "public_key.pem"
    with open(public_path, "wb") as f:
        f.write(
            public_key.public_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PublicFormat.SubjectPublicKeyInfo
            )
        )

    print(f"Created key pair:\n  Private key: {private_path}\n  Public key: {public_path}")

def load_public_key_pem_string(path: str) -> str:
    """Load a PEM file and return its text as a UTF-8 string"""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Public key file does not exist: {path}")
    return path.read_text(encoding="utf-8")


def load_public_key_object(path: str):
    """Load a PEM file and return a cryptography public key object (for signing/verifying)."""
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Public key file does not exist: {path}")
    pem_bytes = path.read_bytes()
    return serialization.load_pem_public_key(pem_bytes)


def load_public_key_from_string(pem_str: str):
    """Load a public key object from a PEM string """
    pem_bytes = pem_str.encode("utf-8")
    return serialization.load_pem_public_key(pem_bytes)


def canonicalize_json(data: dict) -> str:
    """Sort keys recursively for canonical form"""
    return json.dumps(data, sort_keys=True, separators=(",", ":"))

def sign_artifact_json(artifact: dict, private_key) -> None:
    """
    Sign a JSON artifact in-place using deterministic PKCS1v15.
    Updates the artifact with a 'VM_signature' field.
    """
    artifact.pop("signature", None)
    canonical_str = canonicalize_json(artifact).encode("utf-8")
    # Deterministic PKCS1v15 signature
    signature = private_key.sign(
        canonical_str,
        padding.PKCS1v15(),
        hashes.SHA256()
    )

    artifact["signature"] = signature.hex()

def sign_folder(folder_path: Path, private_key):
    pass
    # for file_path in folder_path.glob("*.json"):
    #     sign_artifact(file_path, private_key)


def verify_signature(data, signature, public_key):

    try:
        public_key.verify(
            signature,data,
            padding.PKCS1v15(),
            hashes.SHA256()
        )
        print("Signature is valid")
    except Exception as e:
        print("Signature verification failed:", e)
