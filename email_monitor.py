"""
EmailMonitor Plugin for Cheshire Cat AI (matteocacciola/cheshirecat-core)

Monitors an IMAP mailbox for incoming and outgoing emails and stores
their content into the Cat's declarative vector memory.
"""
import imaplib
import email
import email.header
import time
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Dict, List, Any, Tuple

from langchain_core.documents import Document

from cat.core_plugins.white_rabbit.white_rabbit import JobStatus
from cat.log import log
from cat import hook, CheshireCat, BillTheLizard
from cat.db.cruds.settings import crud as crud_settings
from cat.plugins.ccat_email_monitoring.settings import EmailMonitorSettings


def _get_job_id(cat: CheshireCat) -> str:
    return f"ccat_email_monitoring:{cat.agent_key}"


def _get_db_key(cat: CheshireCat) -> str:
    return f"plugins:{_get_job_id(cat)}:seen"


async def _save_seen_uids(cat: CheshireCat, seen: Dict[str, Any]) -> None:
    """Persist already-processed UIDs to disk."""
    try:
        await crud_settings.store(_get_db_key(cat), seen)
    except Exception as e:
        log.error(f"[EmailMonitor] Error saving seen UIDs: {e}")


def _decode_header_value(raw_value: str) -> str:
    """Decode an email header value that may be RFC-2047 encoded."""
    parts = email.header.decode_header(raw_value)
    return " ".join([
        part.decode(charset or "utf-8", errors="replace") if isinstance(part, bytes) else part
        for part, charset in parts
    ])


def _extract_text_body(msg: email.message.Message) -> str:
    """Extract the plain-text body from an email.message.Message object."""
    body_parts = []
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition", ""))
            if content_type == "text/plain" and "attachment" not in disposition:
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    body_parts.append(payload.decode(charset, errors="replace"))
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            body_parts.append(payload.decode(charset, errors="replace"))
    return "\n".join(body_parts).strip()


def _build_document(
    msg: email.message.Message,
    uid: str,
    folder_type: str,  # "inbox" or "sent"
    settings: EmailMonitorSettings,
) -> Document | None:
    """
    Build a LangChain Document from an email message, ready for
    insertion into the Cat's declarative memory.
    """
    try:
        subject = _decode_header_value(msg.get("Subject", "(no subject)"))
        from_addr = _decode_header_value(msg.get("From", ""))
        to_addr = _decode_header_value(msg.get("To", ""))
        cc_addr = _decode_header_value(msg.get("Cc", ""))
        date_str = msg.get("Date", "")

        try:
            date_obj = parsedate_to_datetime(date_str) if date_str else datetime.now(timezone.utc)
        except Exception:
            date_obj = datetime.now(timezone.utc)

        body = _extract_text_body(msg)
        if not body:
            body = "(empty body)"

        # Truncate body if needed
        if len(body) > settings.max_body_length:
            body = body[: settings.max_body_length] + "\n... [truncated]"

        # Build the textual representation stored in memory
        direction = "RECEIVED" if folder_type == "inbox" else "SENT"
        page_content = (
            f"[EMAIL - {direction}]\n"
            f"Date: {date_obj.isoformat()}\n"
            f"From: {from_addr}\n"
            f"To: {to_addr}\n"
            f"Cc: {cc_addr}\n"
            f"Subject: {subject}\n"
            f"\n{body}"
        )

        metadata = {
            "source": settings.memory_source_tag,
            "when": date_obj.timestamp(),
            "email_uid": uid,
            "email_folder": folder_type,
            "email_direction": direction,
            "email_subject": subject,
            "email_from": from_addr,
            "email_to": to_addr,
            "email_date": date_obj.isoformat(),
        }

        return Document(page_content=page_content, metadata=metadata)

    except Exception as e:
        log.error(f"[EmailMonitor] Error building document for UID {uid}: {e}")
        return None


def _fetch_new_emails(
    conn: imaplib.IMAP4,
    folder: str,
    seen_uids: List,
    folder_type: str,
    settings: EmailMonitorSettings,
) -> Tuple[List[Document], List[str]]:
    """
    Select a folder, fetch all unseen UIDs, parse emails and return (list_of_documents, list_of_new_uids).
    """
    documents = []
    new_uids = []

    try:
        status, _ = conn.select(folder, readonly=True)
        if status != "OK":
            log.warning(f"[EmailMonitor] Could not select folder '{folder}'")
            return documents, new_uids

        # Search for ALL messages; we filter already-seen ones ourselves
        status, data = conn.uid("SEARCH", None, "ALL")
        if status != "OK":
            log.warning(f"[EmailMonitor] UID SEARCH failed in folder '{folder}'")
            return documents, new_uids

        all_uids = data[0].split() if data[0] else []
        # Keep only UIDs we have not processed yet
        unseen = [u.decode() for u in all_uids if u.decode() not in seen_uids]

        if not unseen:
            return documents, new_uids

        log.info(f"[EmailMonitor] {len(unseen)} new email(s) found in '{folder}'")

        for uid in unseen:
            try:
                status, msg_data = conn.uid("FETCH", uid, "(RFC822)")
                if status != "OK" or not msg_data or not msg_data[0]:
                    continue

                raw = msg_data[0][1]
                if not isinstance(raw, bytes):
                    continue

                msg = email.message_from_bytes(raw)
                doc = _build_document(msg, uid, folder_type, settings)
                if doc:
                    documents.append(doc)
                    new_uids.append(uid)
            except Exception as e:
                log.error(f"[EmailMonitor] Error fetching UID {uid}: {e}")
    except Exception as e:
        log.error(f"[EmailMonitor] Error accessing folder '{folder}': {e}")

    return documents, new_uids


# ---------------------------------------------------------------------------
# Core monitoring function (called by the scheduler)
# ---------------------------------------------------------------------------

async def _check_mailbox(settings: EmailMonitorSettings, cat: CheshireCat) -> None:
    """
    Connect to the IMAP server, fetch new emails from inbox and sent
    folders, and store them in the Cat's declarative vector memory.
    This function is invoked periodically by the White Rabbit scheduler.
    """
    lizard = BillTheLizard()

    scheduled_job_id = _get_job_id(cat)
    lock_acquired = lizard.white_rabbit.acquire_lock(scheduled_job_id)
    if not lock_acquired:
        return

    log.info("[EmailMonitor] Starting mailbox check...")
    if not settings.imap_username or not settings.imap_password:
        log.warning("[EmailMonitor] IMAP credentials not configured – skipping.")
        lizard.white_rabbit.release_lock(scheduled_job_id)
        return

    # load seen UIDs from the database
    seen = await crud_settings.read(_get_db_key(cat))
    if not seen:
        seen = {"inbox": [], "sent": []}

    # open and authenticate an IMAP connection.
    try:
        if settings.imap_use_ssl:
            conn = imaplib.IMAP4_SSL(settings.imap_host, settings.imap_port)
        else:
            conn = imaplib.IMAP4(settings.imap_host, settings.imap_port)
            conn.starttls()
        conn.login(settings.imap_username, settings.imap_password)
    except Exception as e:
        log.error(f"[EmailMonitor] IMAP connection failed: {e}")
        lizard.white_rabbit.release_lock(scheduled_job_id)
        return

    all_documents: list[Document] = []
    try:
        # --- Inbox ---
        inbox_docs, inbox_new_uids = _fetch_new_emails(
            conn,
            settings.inbox_folder,
            seen.get("inbox", []),
            "inbox",
            settings,
        )
        all_documents.extend(inbox_docs)
        seen["inbox"] = list(set(seen.get("inbox", []) + inbox_new_uids))

        # --- Sent ---
        sent_docs, sent_new_uids = _fetch_new_emails(
            conn,
            settings.sent_folder,
            seen.get("sent", []),
            "sent",
            settings,
        )
        all_documents.extend(sent_docs)
        seen["sent"] = list(set(seen.get("sent", []) + sent_new_uids))
    finally:
        lizard.white_rabbit.release_lock(scheduled_job_id)
        try:
            conn.logout()
        except Exception:
            pass

    if not all_documents:
        log.info("[EmailMonitor] No new emails to store.")
        await _save_seen_uids(cat, seen)
        lizard.white_rabbit.release_lock(scheduled_job_id)
        return

    # Store documents in the declarative (vector) memory via the RabbitHole
    log.info(f"[EmailMonitor] Storing {len(all_documents)} email(s) into declarative memory...")
    try:
        # Use the RabbitHole ingestion pipeline to chunk and embed the documents
        await cat.rabbit_hole.store_documents(
            docs=all_documents, source=settings.memory_source_tag, file_hash=None, metadata={}
        )
        await _save_seen_uids(cat, seen)
        log.info("[EmailMonitor] Email(s) successfully stored in memory.")
    except Exception as e:
        log.error(f"[EmailMonitor] Error storing documents in memory: {e}")
    finally:
        lizard.white_rabbit.release_lock(scheduled_job_id)


async def _setup_email_monitor_schedule(cat: CheshireCat, job_id: str) -> None:
    """Setup or update the White Rabbit scheduled job for EmailMonitoring."""
    raw_settings = await cat.mad_hatter.get_plugin().load_settings()
    try:
        settings = EmailMonitorSettings(**raw_settings)
        interval_minutes = settings.poll_interval_minutes
        # If no command is configured, just remove the job and return
        if not interval_minutes:
            log.debug("No scheduled EmailMonitor interval configured, skipping job setup")
            return

        log.info(f"EmailMonitor Plugin activated. Scheduling mailbox check every {interval_minutes} minute(s).")

        lizard = cat.lizard

        # Avoid adding the same job twice
        if lizard.white_rabbit.get_job(job_id):
            log.debug(f"EmailMonitor job '{job_id}' already scheduled for CheshireCat '{cat.agent_key}'")
            return

        lizard.white_rabbit.schedule_interval_job(
            job=_check_mailbox,
            job_id=job_id,
            minutes=interval_minutes,
            settings=settings,
            cat=cat,
        )

        log.info(f"EmailMonitor scheduled every {interval_minutes} minute(s) for CheshireCat '{cat.agent_key}'")
    except Exception as e:
        log.error(f"Failed to setup scheduled EmailMonitor job: {str(e)}")


def _remove_email_monitor_schedule(job_id: str) -> None:
    log.info("EmailMonitor Plugin: removing scheduled job.")
    lizard = BillTheLizard()

    # Wait for any currently running execution to finish before replacing the job
    while True:
        job = lizard.white_rabbit.get_job(job_id)

        if not job:
            return  # No job exists, nothing to remove

        if job.status != JobStatus.RUNNING:
            lizard.white_rabbit.remove_job(job_id)
            return

        log.debug(f"EmailMonitor job '{job_id}' is still running, waiting before replacing...")
        time.sleep(5)


@hook(priority=1)
async def after_plugin_toggling_on_agent(plugin_id: str, cat: CheshireCat) -> None:
    """
    Schedule the periodic mailbox check when the plugin is activated.
    """
    if plugin_id != cat.mad_hatter.get_plugin().id:
        return

    job_id = _get_job_id(cat)

    if plugin_id in cat.mad_hatter.active_plugins:
        await _setup_email_monitor_schedule(cat, job_id)
        return

    _remove_email_monitor_schedule(job_id)


@hook(priority=0)
async def after_plugin_settings_update(plugin_id: str, settings: Dict[str, Any], cat: CheshireCat) -> None:
    """Hook called when plugin settings are updated — replaces the cron job with the new config."""
    if plugin_id != cat.mad_hatter.get_plugin().id:
        return

    job_id = _get_job_id(cat)

    # Remove the existing job
    _remove_email_monitor_schedule(job_id)

    # Schedule a fresh job with the updated settings
    await _setup_email_monitor_schedule(cat, job_id)
