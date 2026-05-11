# =============================================================
#  crypto_utils.py  —  AES-256-GCM Encryption / Decryption
#  Location: cloud_file_transfer/server/crypto_utils.py
# =============================================================

import os
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes


class CryptoUtils:
    """
    Handles all encryption and decryption for the CloudFS system.
    Uses AES-256-GCM (Galois/Counter Mode) which provides:
      - Confidentiality  : data cannot be read without the key
      - Integrity        : any tampering is detected via auth tag
    """

    def __init__(self, keys_dir: str):
        self.keys_dir = keys_dir
        os.makedirs(keys_dir, exist_ok=True)

    # ----------------------------------------------------------
    #  Key Management
    # ----------------------------------------------------------

    def generate_key(self):
        """
        Generate a cryptographically random 256-bit (32-byte) AES key.
        Returns:
            key     (bytes) : raw 32-byte key
            key_hex (str)   : hexadecimal string representation (64 chars)
        """
        key = get_random_bytes(32)          # 32 bytes = 256 bits
        return key, key.hex()

    def save_key(self, file_id: str, key: bytes):
        """Persist the raw AES key to disk under keys/<file_id>.key"""
        path = os.path.join(self.keys_dir, f"{file_id}.key")
        with open(path, "wb") as f:
            f.write(key)

    def load_key(self, file_id: str) -> bytes:
        """Load raw AES key bytes from disk. Returns None if not found."""
        path = os.path.join(self.keys_dir, f"{file_id}.key")
        if not os.path.exists(path):
            return None
        with open(path, "rb") as f:
            return f.read()

    def delete_key(self, file_id: str):
        """Permanently delete a key file (called when file is deleted)."""
        path = os.path.join(self.keys_dir, f"{file_id}.key")
        if os.path.exists(path):
            os.remove(path)

    # ----------------------------------------------------------
    #  Encryption
    # ----------------------------------------------------------

    def encrypt_block(self, plaintext: bytes, key: bytes):
        """
        Encrypt a single data block using AES-256-GCM.

        How it works:
          1. Generate a random 16-byte nonce (number used once)
          2. Create AES-GCM cipher with key + nonce
          3. Encrypt plaintext → ciphertext
          4. Produce 16-byte authentication tag

        The nonce MUST be unique for every encryption with the same key.
        We use get_random_bytes(16) to guarantee this.

        Args:
            plaintext (bytes) : raw block data
            key       (bytes) : 32-byte AES key

        Returns:
            ciphertext (bytes) : encrypted block data
            nonce      (bytes) : 16-byte random nonce
            tag        (bytes) : 16-byte authentication tag
        """
        nonce  = get_random_bytes(16)
        cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
        ciphertext, tag = cipher.encrypt_and_digest(plaintext)
        return ciphertext, nonce, tag

    # ----------------------------------------------------------
    #  Decryption
    # ----------------------------------------------------------

    def decrypt_block(self, ciphertext: bytes, key: bytes,
                      nonce: bytes, tag: bytes) -> bytes:
        """
        Decrypt and verify a single data block using AES-256-GCM.

        How it works:
          1. Recreate cipher with same key + nonce used during encryption
          2. Decrypt ciphertext → plaintext
          3. Verify authentication tag — raises ValueError if tampered

        Args:
            ciphertext (bytes) : encrypted block data
            key        (bytes) : 32-byte AES key
            nonce      (bytes) : 16-byte nonce (stored alongside block)
            tag        (bytes) : 16-byte auth tag (stored alongside block)

        Returns:
            plaintext (bytes) : original decrypted data

        Raises:
            ValueError : if tag verification fails (data was tampered)
        """
        cipher = AES.new(key, AES.MODE_GCM, nonce=nonce)
        plaintext = cipher.decrypt_and_verify(ciphertext, tag)
        return plaintext
