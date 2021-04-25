import logging
import base64
from typing import Dict

log = logging.getLogger(__name__)


class KmsDao:

    def __init__(self, boto_kms_client):
        self._kms = boto_kms_client

    def decrypt(self, base64_ciphertext):
        ciphertext = base64.b64decode(base64_ciphertext)
        return self._kms.decrypt(CiphertextBlob=ciphertext)[u"Plaintext"].decode()

    def decrypt_with_context(self, base64_ciphertext, context: Dict):
        ciphertext = base64.b64decode(base64_ciphertext)
        return self._kms.decrypt(
            CiphertextBlob=ciphertext,
            EncryptionContext=context)[u"Plaintext"].decode()
