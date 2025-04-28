import gzip
import os
import base64
import logging
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import padding


log = logging.getLogger()


class Crypto:

    @classmethod
    def _unpad(self, s): return s[:-ord(s[len(s)-1:])]

    @classmethod
    def encrypt(self, raw, key, encoding='utf-8'):
        backend = default_backend()
        iv = os.urandom(16)
        compressed = gzip.compress(bytes(raw, encoding=encoding))
        padder = padding.PKCS7(algorithms.AES.block_size).padder()
        padded_data = padder.update(compressed) + padder.finalize()
        cipher = Cipher(algorithms.AES(base64.b64decode(key)),
                        modes.CBC(iv),
                        backend=backend)
        encryptor = cipher.encryptor()
        encrypted = encryptor.update(padded_data) + encryptor.finalize()
        return f'{bytes.hex(iv)}:{bytes.hex(encrypted)}'

    @classmethod
    def decrypt(self, enc, keys):
        for key in keys:
            try:
                backend = default_backend()
                iv, message = enc.split(':')
                iv = bytes.fromhex(iv)
                message = bytes.fromhex(message)
                cipher = Cipher(algorithms.AES(base64.b64decode(key)),
                                modes.CBC(iv),
                                backend=backend)
                decryptor = cipher.decryptor()
                padded = decryptor.update(message)
                compressed = self._unpad(padded)
                data = gzip.decompress(compressed)
                return data
            except Exception as e:
                log.error(f'Decrypt key error: {e}')
                pass

        raise Exception('Unable to decrypt. Tried every key.')
