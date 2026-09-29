from typing import Any, Dict, List, Protocol, Tuple

#: settings encrypted at rest: every field whose key contains "_secret"
SECRET_SETTINGS = ("imap_password",)


class Crypto(Protocol):
    def encrypt(self, plaintext: str) -> str: ...

    def decrypt(self, ciphertext: str) -> str: ...


def encrypt_secrets(settings: Dict[str, Any], crypto: Crypto) -> Dict[str, Any]:
    """Copy of ``settings`` with the non-empty secrets encrypted (empty means not configured)."""
    return {
        k: crypto.encrypt(v) if k in SECRET_SETTINGS and isinstance(v, str) and v else v
        for k, v in settings.items()
    }


def decrypt_secrets(settings: Dict[str, Any], crypto: Crypto) -> Tuple[Dict[str, Any], List[str]]:
    """Copy of ``settings`` with the secrets decrypted, and the keys that could not be decrypted.

    An undecryptable secret (e.g. ``CAT_CRYPTO_KEY`` changed) becomes empty: the feature it
    enables is off until the secret is saved again, and the rest of the settings keeps working.
    """
    decrypted = dict(settings)
    failed: List[str] = []
    for key in SECRET_SETTINGS:
        value = decrypted.get(key)
        if not isinstance(value, str) or not value:
            continue
        try:
            decrypted[key] = crypto.decrypt(value)
        except Exception:  # noqa: BLE001 - Fernet raises InvalidToken, base64 raises ValueError
            decrypted[key] = ""
            failed.append(key)
    return decrypted, failed
