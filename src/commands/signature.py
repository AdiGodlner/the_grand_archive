from cli_core import command
from signature_manager import create_key_pair as _create_key_pair

@command()
def create_key_pair(output_folder, password: str = None):
    """Generate RSA key pair."""
    return _create_key_pair(output_folder, password)

