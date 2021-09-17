import logging
import base64
from typing import Dict

from figgy.constants.data import ENCRYPTION_CONTEXT_ENCRYPTOR_KEY, ENCRYPTION_CONTEXT_PASSWORD_KEY, \
    ENCRYPTION_CONTEXT_ENCRYPTOR_DEFAULT_VALUE

log = logging.getLogger(__name__)


class KmsDao:

    def __init__(self, boto_kms_client):
        self._kms = boto_kms_client

    def decrypt(self, base64_ciphertext, encryption_password=None):
        ciphertext = base64.b64decode(base64_ciphertext)
        context = None

        if encryption_password:
            context = self.__build_context(encryption_password)

        return self._kms.decrypt(CiphertextBlob=ciphertext, EncryptionContext=context)[u"Plaintext"].decode()

    def decrypt_with_context(self, base64_ciphertext, context: Dict):
        ciphertext = base64.b64decode(base64_ciphertext)
        return self._kms.decrypt(
            CiphertextBlob=ciphertext,
            EncryptionContext=context)[u"Plaintext"].decode()

    def encrypt(self, key_id: str, value: str, encryption_password: str = None) -> bytes:
        response = self._kms.encrypt(
            KeyId=key_id,
            Plaintext=value,
            EncryptionContext=self.__build_context(encryption_password),
            EncryptionAlgorithm='SYMMETRIC_DEFAULT'
        )
        cipher_text: bytes = response.get('CiphertextBlob')

        if not cipher_text:
            raise Exception(f"Encryption failed using kms_key: {key_id}")

        return cipher_text

    @staticmethod
    def __build_context(encryption_password: str):
        return {
            ENCRYPTION_CONTEXT_ENCRYPTOR_KEY: ENCRYPTION_CONTEXT_ENCRYPTOR_DEFAULT_VALUE,
            ENCRYPTION_CONTEXT_PASSWORD_KEY: encryption_password
        }
