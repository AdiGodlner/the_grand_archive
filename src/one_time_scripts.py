import json
import os

from signature_manager import load_public_key_pem_string, SignaturesManager, sign_artifact_json


def on_build_sign_publishers():
    #  first load json  then go over each museum in the list then do
    # TODO maybe put this in a separate script or find a better way
    # TODO rewrite this method this is a mess
    # TODO
    #  to check the signature remove the signature from the object
    #  sign the object with the relevant public key
    #  check that the existing signature matches the new one

    keys_dir = os.path.abspath("../../keys/")
    trusted_publishers_path = os.path.abspath("./trusted_publishers.json")

    with open(trusted_publishers_path , "r", encoding="utf-8") as f:
        data = json.load(f)

    trusted_publishers = data["trusted_publishers"]
    for publisher in trusted_publishers:
        publisher_id = publisher["publisher_id"]
        pub_key_path = os.path.join(keys_dir, publisher_id, "public_key.pem")
        print(pub_key_path)
        pub_key = load_public_key_pem_string(pub_key_path)
        publisher["public_key"] = pub_key

    # load the grand archive public key
    pub_key_path = os.path.join(keys_dir, "thegrandarchive.com", "public_key.pem")
    pub_key = load_public_key_pem_string(pub_key_path)
    data["public_key"] = pub_key
    manager = SignaturesManager()
    private_key = manager.load_private_key("../../keys/thegrandarchive.com/private_key.pem")
    sign_artifact_json(data, private_key)

    with open(trusted_publishers_path , "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    verfiy_signature(data)
    print("done")


def verfiy_signature(data):

    json.pop()

