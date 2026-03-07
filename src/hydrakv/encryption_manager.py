import base64
import pickle
from pathlib import Path
from typing import Dict, Optional, Tuple
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey, RSAPublicKey
from loguru import logger


class EncryptionManager:
    """
    Creates a EncryptionManager instance for managing encryption operations.
    """

    def __init__(self, server_name: str, secret_key: bytes, key_path: Optional[Path] = None) -> None:
        """
        Initializes the EncryptionManager with a server name and optional key path.

        Args:
            server_name: Name of the server
            secret_key: Password for encryption
            key_path: Optional path to the key file

        Returns: None
        """
        self._server_name: str = server_name
        self._keyfile: Dict = self._load_key_file(Path(f"./.keys/{self._server_name}.key"), secret_key)

        # if the keyfile is not existing, we create a new keyfile with publich and private key
        if self._keyfile is None:
            self._keyfile = self._create_key_file(key_path, secret_key)

        # if the name saved in the keyfile does not match the server name, issue a warning
        if self._keyfile["server_name"] != self._server_name:
            logger.warning(
                f"Server name in keyfile ({self._keyfile['server_name']}) does not match server name ({self._server_name})!")

    def _load_key_file(self, path: Optional[Path], pw: bytes) -> Optional[Dict]:
        """
        if present, loads keyfile from given path

        Args:
            path: Optional Path to load the keyfile from

        Returns: Dict with public and private key or None if not existing
        """

        # if path exists, load keyfile
        if path.exists():
            with open(path, "rb") as keyfile:
                pem_private = keyfile.read()

            private_key = serialization.load_pem_private_key(pem_private, password=pw)
            public_key = private_key.public_key()

            return {
                "server_name": self._server_name,
                "private_key": private_key,
                "public_key": public_key
            }
        return None

    def _create_key_file(self, path: Optional[Path], pw: bytes) -> Dict:
        """
        Creates a new keyfile with public and private key and saves it to the given path.
        Args:
            path: Optional Path to save the keyfile
            pw: bytes password for encryption

        Returns: Dict with public and private key
        """

        # if path is not given, create a default path
        if path is None:
            path = Path(f"./.keys/{self._server_name}.key")

        # create the key pair
        key_pair: Tuple[RSAPrivateKey, RSAPublicKey] = self._create_key_pair()  # if this fails, we raise an exception

        # create the dict, save it under the given path
        key_dict = {
            "server_name": self._server_name,
            "private_key": key_pair[0],
            "public_key": key_pair[1]
        }

        # cerate the path if not existing
        path.parent.mkdir(parents=True, exist_ok=True)

        # write private key to file (with password protection)
        pem_private = key_pair[0].private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.BestAvailableEncryption(pw))

        with open(path, "wb") as keyfile:
            pickle.dump(pem_private, keyfile)

        # write public key to file
        pem_public = key_pair[1].public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo)

        with open(path.with_suffix(".pub"), "wb") as keyfile:
            pickle.dump(pem_public, keyfile)

        logger.info(f"Successfully created key file at {path}")
        return key_dict

    def _create_key_pair(self) -> Tuple[RSAPrivateKey, RSAPublicKey]:
        """
        Create private and public key pair

        Returns: Tuple with private and public key

        Raises: Exception if key creation fails
        """
        try:
            private_key: RSAPrivateKey = rsa.generate_private_key(public_exponent=65537, key_size=4096)
            public_key: RSAPublicKey = private_key.public_key()
            logger.info("Successfully created new key pair")
        except Exception as e:
            logger.error(f"Failed to create key pair: {e}")
            raise
        return private_key, public_key

    def encrypt(self, data: str) -> str:
        """
        Encrypts the data and returns it as a base64 encoded string.

        Args:
            data: The data to be encrypted

        Returns:
            Encrypted data as base64 encoded string

        Raises:
            Exception if encryption fails
        """

        try:
            ct = self._keyfile['public_key'].encrypt(
                data.encode(),
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            return base64.b64encode(ct).decode()
        except Exception as e:
            logger.error(f"Failed to encrypt data: {e}")
            raise

    def decrypt(self, data: str) -> str:
        """
        Decrypts the data expected as a base64 encoded string.

        Args:
            data: The data to be decrypted

        Returns:
            Decrypted data as string

        Raises:
            Exception if decryption fails
        """

        try:
            pt = self._keyfile['private_key'].decrypt(
                base64.b64decode(data),
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            return pt.decode()
        except Exception as e:
            logger.error(f"Failed to decrypt data: {e}")
            raise
