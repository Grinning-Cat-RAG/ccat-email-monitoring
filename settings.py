from typing import Dict, Any
from pydantic import BaseModel, Field

from cat import plugin, log
from cat.db.cruds import plugins as crud_plugins
from cat.services.string_crypto import StringCrypto

from .crypt import decrypt_secrets, encrypt_secrets


class EmailMonitorSettings(BaseModel):
    """Settings for the Email Monitor plugin."""
    # IMAP connection (for reading inbox AND sent folder)
    imap_host: str = Field(
        title="IMAP Host",
        description="Hostname of the IMAP server (e.g. imap.gmail.com)",
        default="imap.gmail.com",
    )
    imap_port: int = Field(
        title="IMAP Port",
        description="Port for the IMAP server (993 for SSL, 143 for STARTTLS)",
        default=993,
    )
    imap_use_ssl: bool = Field(
        title="IMAP Use SSL",
        description="Use SSL/TLS for the IMAP connection",
        default=True,
    )
    imap_username: str = Field(
        title="IMAP Username",
        description="Email address / username for IMAP login",
        default="",
    )
    imap_password: str = Field(
        title="IMAP Password",
        description="Password (or App Password) for IMAP login",
        default="",
        json_schema_extra={"type": "password"},
    )

    # Folders to monitor
    inbox_folder: str = Field(
        title="Inbox Folder",
        description="IMAP folder name for received emails",
        default="INBOX",
    )
    sent_folder: str = Field(
        title="Sent Folder",
        description='IMAP folder name for sent emails (e.g. "Sent", "[Gmail]/Sent Mail")',
        default="[Gmail]/Sent Mail",
    )

    # Polling interval
    poll_interval_minutes: int = Field(
        title="Poll Interval (minutes)",
        description="How often (in minutes) to check for new emails",
        default=5,
        ge=1,
        le=1440,
    )

    # Memory source tag
    memory_source_tag: str = Field(
        title="Memory Source Tag",
        description="Metadata tag used to identify emails in the vector memory",
        default="email_monitor",
    )

    # Maximum body length stored per email (chars)
    max_body_length: int = Field(
        title="Max Body Length (chars)",
        description="Maximum number of characters of the email body to store in memory",
        default=4000,
        ge=100,
        le=50000,
    )


def _decrypted(stored: Dict[str, Any], agent_id: str) -> Dict[str, Any]:
    settings, failed = decrypt_secrets(stored, StringCrypto())
    for key in failed:
        log.error(
            f"[connectors] agent {agent_id}: cannot decrypt '{key}' (was CAT_CRYPTO_KEY changed?): "
            "it is ignored until saved again"
        )
    return settings


@plugin
def settings_schema():
    return EmailMonitorSettings.model_json_schema()


@plugin
async def load_settings(plugin_id: str, agent_id: str) -> Dict[str, Any]:
    stored = await crud_plugins.get_setting(agent_id, plugin_id)
    if stored is None:
        return EmailMonitorSettings().model_dump()
    return _decrypted(stored, agent_id)


@plugin
async def save_settings(plugin_id: str, settings: Dict[str, Any], agent_id: str) -> Dict[str, Any]:
    stored = await crud_plugins.update_setting(agent_id, plugin_id, encrypt_secrets(settings, StringCrypto()))
    return _decrypted(stored, agent_id)
