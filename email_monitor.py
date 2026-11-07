"""
EmailMonitor Plugin for Cheshire Cat AI (matteocacciola/cheshirecat-core)

Monitors an IMAP mailbox for incoming and outgoing emails and stores
their content into the Cat's declarative vector memory.
"""
import base64
import binascii
import codecs
import imaplib
import email
import email.header
import asyncio
import quopri
import re
from datetime import datetime, timezone
from email.message import Message
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import Dict, List, Any, Tuple

from langchain_core.documents import Document

from cat.core_plugins.white_rabbit.white_rabbit import JobStatus
from cat.log import log
from cat import hook, CheshireCat, BillTheLizard
from cat.db.cruds.settings import crud as crud_settings
from .settings import EmailMonitorSettings


# while the mailbox check of the agent runs, its replacement checks it again after this many seconds
JOB_POLL_SECONDS = 5


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


# charset labels read as the WHATWG Encoding standard (the browsers, the mail clients) reads them, where Python
# reads them otherwise or does not know them
_CHARSET_ALIASES = {
    **dict.fromkeys(("iso-8859-8-i", "iso-8859-8-e", "csiso88598i", "csiso88598e", "logical", "visual"),
                    "iso-8859-8"),
    **dict.fromkeys(("iso-8859-6-i", "iso-8859-6-e", "csiso88596i", "csiso88596e"), "iso-8859-6"),
    **dict.fromkeys(("iso-8859-9", "iso8859-9", "iso_8859-9", "latin5", "l5", "csisolatin5"), "cp1254"),
    **dict.fromkeys(("windows-874", "dos-874", "iso-8859-11", "iso8859-11", "iso885911", "tis-620"), "cp874"),
    **dict.fromkeys(("x-mac-roman", "macintosh", "mac", "csmacintosh"), "mac_roman"),
    **dict.fromkeys(("x-mac-cyrillic", "x-mac-ukrainian"), "mac_cyrillic"),
    **dict.fromkeys(("shift_jis", "shift-jis", "sjis", "x-sjis", "ms_kanji", "windows-31j", "csshiftjis", "ms932"),
                    "cp932"),
    **dict.fromkeys(("gb2312", "gb_2312", "gb_2312-80", "x-gbk", "gbk", "chinese", "csgb2312", "csiso58gb231280",
                     "iso-ir-58"), "gbk"),
    **dict.fromkeys(("big5", "big5-hkscs", "cn-big5", "csbig5", "x-x-big5"), "big5hkscs"),
    **dict.fromkeys(("euc-kr", "ks_c_5601-1987", "ks_c_5601-1989", "ksc5601", "ksc_5601", "korean",
                     "csksc56011987", "iso-ir-149", "windows-949"), "cp949"),
    **dict.fromkeys(("koi8-u", "koi8-ru"), "koi8_u"),
    **dict.fromkeys(("utf8", "unicode-1-1-utf-8", "unicode11utf8", "unicode20utf8", "x-unicode20utf8"), "utf-8"),
}
# labels often put on text that is not in them: valid UTF-8 is read as UTF-8, anything else as windows-1252 (the
# WHATWG reading of these labels)
_LOOSE_CHARSETS = frozenset({
    "us-ascii", "ascii", "us", "ansi_x3.4-1968", "iso646-us", "iso-ir-6", "cp367", "ibm367", "csascii",
    "iso-8859-1", "iso8859-1", "iso88591", "iso_8859-1", "iso_8859-1:1987", "latin1", "latin-1", "l1", "cp819",
    "ibm819", "iso-ir-100", "csisolatin1",
})
# codecs of Python that are not charsets of text (they would rewrite it)
_NOT_CHARSETS = frozenset({"idna", "punycode", "unicode-escape", "raw-unicode-escape", "undefined", "mbcs", "oem"})


def _decode_loose(data: bytes) -> str:
    """Bytes of an unknown or untrusted charset: UTF-8 when they are valid UTF-8, else windows-1252."""
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("cp1252", errors="replace")


def _decode_bytes(data: bytes, charset: str | None) -> str:
    """
    Decode ``data`` with the declared charset, read as the WHATWG Encoding standard reads it. A missing, unknown or
    non-text one (e.g. ``x-unknown``, ``base64``), an ASCII or latin-1 one: UTF-8 if the bytes are valid UTF-8,
    else windows-1252. Bytes undecodable in a declared charset become U+FFFD: the text is never lost for a bad label.
    """
    label = (charset or "").strip().strip("\"'").lower()
    if not label or label in _LOOSE_CHARSETS:
        return _decode_loose(data)
    try:
        codec = codecs.lookup(_CHARSET_ALIASES.get(label, label))
        if codec.name in _NOT_CHARSETS:
            return _decode_loose(data)
        return data.decode(codec.name, errors="replace")
    except (LookupError, ValueError):  # ValueError: UnicodeError of codecs, null bytes in the name
        return _decode_loose(data)


# an RFC 2047 encoded word: =?charset[*language]?B|Q?text?=
_ENCODED_WORD = re.compile(r"=\?([^?\s*]+)(?:\*[^?\s]*)?\?([bBqQ])\?([^?\s]*)\?=")


def _word_bytes(match: re.Match) -> Tuple[str, bytes] | None:
    """(charset, bytes) of an encoded word, ``None`` when its payload is malformed (it is then kept as it is)."""
    charset, encoding, payload = match.groups()
    try:
        if encoding in "bB":
            data = base64.b64decode(payload + "=" * (-len(payload) % 4), validate=True)
        else:  # Q: "_" is a space, =XX a byte
            data = quopri.decodestring(payload.replace("_", " ").encode("ascii"), header=True)
    except (binascii.Error, ValueError):
        return None
    return charset.lower(), data


def _decode_header_value(raw_value: str | email.header.Header | None) -> str:
    """
    Decode an email header value that may be RFC-2047 encoded. Never raises: an encoded word that cannot be
    decoded is kept as it is; charsets are read as ``_decode_bytes`` reads them, raw 8-bit bytes (a ``Header`` of
    the parser) as a missing one. Folded lines are unfolded; the whitespace between two encoded words is dropped
    (RFC 2047), any other whitespace is kept; the bytes of adjacent encoded words in the same charset are decoded
    together (some mailers split a character between two of them).
    """
    if raw_value is None:
        return ""
    if isinstance(raw_value, email.header.Header):  # raw 8-bit bytes in the header
        raw_value = "".join(
            _decode_bytes(chunk, charset) if isinstance(chunk, bytes) else chunk
            for chunk, charset in email.header.decode_header(raw_value)
        )
    value = re.sub(r"\r?\n(?=[ \t])", "", str(raw_value))
    out: List[str] = []
    pending: Tuple[str, bytes] | None = None  # the bytes of the encoded words not decoded yet

    def flush() -> None:
        nonlocal pending
        if pending is not None:
            out.append(_decode_bytes(pending[1], pending[0]))
            pending = None

    position = 0
    for match in _ENCODED_WORD.finditer(value):
        between = value[position:match.start()]
        word = _word_bytes(match)
        if word is not None and pending is not None and not between.strip():  # adjacent to the previous word
            if word[0] == pending[0]:
                pending = (pending[0], pending[1] + word[1])
            else:
                flush()
                pending = word
        else:
            flush()
            out.append(between)
            if word is None:
                out.append(match.group(0))
            else:
                pending = word
        position = match.end()
    flush()
    out.append(value[position:])
    return "".join(out)


# HTML elements whose content is not text of the body. Not <head>: often left open, the browsers close it at
# <body>; what it holds that is not text (title, styles, scripts) is dropped by these
_HTML_SKIPPED = frozenset({"script", "style", "template", "title"})
# HTML elements that start a new paragraph, or a new line (as the browsers show them: Gmail and Apple Mail put each
# typed line in a <div>, their blank line is <div><br></div>)
_HTML_PARAGRAPHS = frozenset({
    "address", "article", "aside", "blockquote", "dd", "dl", "dt", "fieldset", "figure", "footer", "form",
    "h1", "h2", "h3", "h4", "h5", "h6", "header", "hr", "main", "nav", "p", "pre", "section", "table",
})
_HTML_LINES = frozenset({  # and "br", that adds up
    "caption", "center", "details", "div", "figcaption", "legend", "li", "menu", "ol", "option", "summary", "tr", "ul",
})
# HTML elements separated by a space from the text before them
_HTML_CELLS = frozenset({"td", "th"})
# link targets that are not addresses for the reader
_HTML_LOCAL_LINK = re.compile(r"\s*(#|javascript:|$)", re.IGNORECASE)


def _link_key(text: str) -> str:
    """A link text or target without scheme and final slash: "https://x.it/" and "x.it" are the same address."""
    return re.sub(r"^(https?://|mailto:)", "", text.strip().lower()).rstrip("/")


class _HtmlText(HTMLParser):
    """Plain text of an HTML body: entities decoded, scripts, styles, templates and title dropped, whitespace
    collapsed outside ``<pre>`` (kept as it is inside, but the line end right after ``<pre>``), block elements
    turned into line or paragraph breaks, the address of a link after its text (once, when the text is not
    already the address), images replaced by their alternative text."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        # (kind, text): "text" (whitespace collapsed), "pre" (kept as it is) or "break" (only line ends)
        self.chunks: List[Tuple[str, str]] = []
        self.skipped = 0  # depth inside _HTML_SKIPPED elements
        self.pre = 0  # depth inside <pre>
        self.pre_opened = False  # right after <pre>: its first line end is dropped
        self.links: List[Tuple[str | None, int]] = []  # the open <a>: (target, index of their first chunk)

    def _break(self, newlines: int, adding: bool = False) -> None:
        """A line (1) or paragraph (2) break. Consecutive block breaks do not add up (``</li><li>`` is one new line,
        ``</p><p>`` one blank line); ``<br>`` breaks do (``<br><br>`` is a blank line), never beyond one blank line."""
        if self.chunks and self.chunks[-1][0] == "break":
            current = len(self.chunks[-1][1])
            self.chunks[-1] = ("break", "\n" * min(2, current + newlines if adding else max(current, newlines)))
            return
        if self.chunks and self.chunks[-1][0] == "text":
            self.chunks[-1] = ("text", self.chunks[-1][1].rstrip(" "))  # no space at the end of a line
        self.chunks.append(("break", "\n" * newlines))

    def _after_space(self) -> bool:
        """At the start of the text, or right after a space or a break."""
        return not self.chunks or self.chunks[-1][1][-1:].isspace() or self.chunks[-1][0] == "break"

    def _tag(self, tag: str, opening: bool, attrs: List[Tuple[str, str | None]] = ()) -> None:
        if tag in _HTML_SKIPPED:
            self.skipped = self.skipped + 1 if opening else max(0, self.skipped - 1)
            return
        if self.skipped:  # e.g. the <p> of a <template>
            return
        self.pre_opened = False
        if tag == "pre":
            self.pre = self.pre + 1 if opening else max(0, self.pre - 1)
            self.pre_opened = opening
        if tag in _HTML_PARAGRAPHS:
            self._break(2)
        elif tag == "br":  # </br> too: the browsers read it as <br>
            self._break(1, adding=True)
        elif tag in _HTML_LINES:
            self._break(1)
        elif tag in _HTML_CELLS and opening and not self._after_space():
            self.chunks.append(("text", " "))
        elif tag == "img" and opening:
            self.handle_data(dict(attrs).get("alt") or "")
        elif tag == "a" and opening:
            self.links.append((dict(attrs).get("href"), len(self.chunks)))
        elif tag == "a" and self.links:
            self._close_link()

    def _close_link(self) -> None:
        target, first = self.links.pop()
        if not target or _HTML_LOCAL_LINK.match(target):
            return
        target = target.strip()
        shown = "".join(chunk for kind, chunk in self.chunks[first:] if kind != "break")
        if not shown.strip():
            self.handle_data(target)
        elif _link_key(shown) != _link_key(target):
            self.handle_data(f" ({target})")

    def handle_starttag(self, tag, attrs):
        self._tag(tag, True, attrs)

    def handle_endtag(self, tag):
        self._tag(tag, False)

    def handle_startendtag(self, tag, attrs):
        # <br/>, <p/>, <img/>: one break, the alternative text; <script/>: opens and closes, nothing skipped after it
        if tag not in _HTML_SKIPPED:
            self._tag(tag, True, attrs)

    def handle_data(self, data):
        if self.skipped:
            return
        if self.pre:  # line ends already normalised by _part_text
            if self.pre_opened and data.startswith("\n"):
                data = data[1:]
            self.pre_opened = False
            if data:
                self.chunks.append(("pre", data))
            return
        text = re.sub(r"\s+", " ", data)
        if self._after_space():
            text = text.lstrip(" ")
        if text:
            self.chunks.append(("text", text))

    def text(self) -> str:
        out: List[str] = []
        trailing = 0  # line ends at the end of the text so far
        for kind, chunk in self.chunks:
            if kind == "break":  # the line ends already at the end of preformatted text count
                chunk = chunk[trailing:]
            if not chunk:
                continue
            out.append(chunk)
            stripped = len(chunk.rstrip("\n"))
            trailing = trailing + len(chunk) if not stripped else len(chunk) - stripped
        text = "".join(out).strip("\n")
        # spaces at the two ends: only preformatted text keeps them
        written = [kind for kind, chunk in self.chunks if kind != "break" and chunk]
        if written and written[0] != "pre":
            text = text.lstrip(" ")
        if written and written[-1] != "pre":
            text = text.rstrip(" ")
        return text


def _html_to_text(markup: str) -> str:
    """Plain text of an HTML document (malformed markup included: the parser of the stdlib is lenient)."""
    parser = _HtmlText()
    parser.feed(markup)
    parser.close()
    return parser.text()


def _part_text(part: Message) -> str:
    """Decoded text of a leaf part, with LF line ends (IMAP messages come with CRLF)."""
    payload = part.get_payload(decode=True)
    if not payload:
        return ""
    text = _decode_bytes(payload, part.get_content_charset())
    return text.replace("\r\n", "\n").replace("\r", "\n").strip()


def _body_texts(part: Message) -> List[Tuple[str, bool]]:
    """
    The texts of ``part`` that belong to the body, in order, as (text, converted from HTML); only the ones with
    some text. Attachments, and everything inside one (e.g. an attached email), are left out: ``Content-Disposition``
    is parsed (``ATTACHMENT`` is an attachment, an inline part whose file name contains "attachment" is not).
    Of a ``multipart/alternative`` only one choice is kept, the last one made of text/plain parts only, else the
    last one with some text (RFC 2046: the choices are the same content, the last is the richest); every other
    multipart (unknown subtypes included, as RFC 2046 says, and an inline ``message/rfc822``) keeps all its parts,
    but the messages in a ``multipart/report`` (the copy of a bounced email).
    """
    if part.get_content_disposition() == "attachment":
        return []
    if part.is_multipart():
        children = [
            _body_texts(child) for child in part.get_payload()
            # a delivery report holds a copy of the bounced email (already in memory, or not ours): not body
            if not (part.get_content_type() == "multipart/report" and child.get_content_maintype() == "message")
        ]
        if part.get_content_type() != "multipart/alternative":
            return [text for child in children for text in child]
        filled = [child for child in children if child]
        plain = [child for child in filled if not any(from_html for _, from_html in child)]
        return (plain or filled or [[]])[-1]
    content_type = part.get_content_type()
    if content_type == "text/plain":
        text = _part_text(part)
    elif content_type == "text/html":
        text = _html_to_text(_part_text(part))
    else:
        return []
    return [(text, content_type == "text/html")] if text.strip() else []


def _extract_text_body(msg: Message) -> str:
    """
    Text body of an email: its text/plain and text/html parts that are not attachments, the HTML converted to plain
    text, one choice per ``multipart/alternative`` (see ``_body_texts``). Any other part (and any attachment) is
    ignored: an email without a text part has an empty body.
    """
    return "\n\n".join(text for text, _ in _body_texts(msg))


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


async def _remove_email_monitor_schedule(job_id: str) -> None:
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
        # asynchronous: the event loop serves the other requests while the check runs
        await asyncio.sleep(JOB_POLL_SECONDS)


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

    await _remove_email_monitor_schedule(job_id)


@hook(priority=0)
async def after_plugin_settings_update(plugin_id: str, settings: Dict[str, Any], cat: CheshireCat) -> None:
    """Hook called when plugin settings are updated — replaces the cron job with the new config."""
    if plugin_id != cat.mad_hatter.get_plugin().id:
        return

    job_id = _get_job_id(cat)

    # Remove the existing job
    await _remove_email_monitor_schedule(job_id)

    # Schedule a fresh job with the updated settings
    await _setup_email_monitor_schedule(cat, job_id)
