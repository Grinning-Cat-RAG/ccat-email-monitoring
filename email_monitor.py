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
import mimetypes
import os
import quopri
import re
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone
from email.message import Message
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from io import BytesIO
from typing import Dict, List, Any, Tuple

from langchain_core.documents import Document

from cat.log import log
from cat import hook, CheshireCat, BillTheLizard
from cat.db.cruds import ingestions as crud_ingestions
from cat.rabbit_hole.engine import STATE_COMPLETE
from cat.rabbit_hole.phases import PHASE_EMBEDDING
from cat.db.cruds.settings import crud as crud_settings
from cat.db.cruds import plugins as crud_plugins, settings as crud_agents
from cat.utils import guess_file_type
from .settings import EmailMonitorSettings


# the id of the plugin: the name of its folder, as for the core
PLUGIN_ID = os.path.basename(os.path.dirname(os.path.abspath(__file__)))
# the destroy of an agent waits for its running check (its lock) checking it again after this many seconds, at most
# DESTROY_WAIT_SECONDS: a check of a destroyed agent ends at its next file (the core cancels its ingestions), and the
# lock of a check whose node died stays until it expires (an hour)
JOB_POLL_SECONDS = 5
DESTROY_WAIT_SECONDS = 300
# an email that fails this many times is not tried again (its failed attachments, or the whole email, are given up)
MAX_ATTEMPTS = 3
# every operation on the IMAP server (connection included) gives up after this many seconds; imaplib is blocking:
# its calls run in a thread, one at a time, the event loop of the instance keeps serving the other agents and chats
IMAP_TIMEOUT_SECONDS = 60
# the folders checked: (type, name of the setting with the IMAP folder name)
FOLDERS = (("inbox", "inbox_folder"), ("sent", "sent_folder"))
# the collection of the documents
DECLARATIVE = "declarative"
# a service is unreachable (the IMAP server, the vector DB...): the check ends, the email being stored is not a
# failed attempt (it is not its fault) and is tried again at the next check
_CONNECTION_LOST = (TimeoutError, OSError, imaplib.IMAP4.abort)


def _clock() -> float:
    """Seconds of a monotonic clock: the time budget of a check."""
    return time.monotonic()


def _get_job_id(agent_id: str) -> str:
    return f"ccat_email_monitoring:{agent_id}"


def _get_db_key(agent_id: str) -> str:
    """The state of the folders of the agent; out of the keys of the agent the core destroys with it (see
    ``after_cheshire_cat_destroy``)."""
    return f"plugins:{_get_job_id(agent_id)}:seen"


async def _save_state(cat: CheshireCat, state: Dict[str, Any]) -> None:
    """Persist the state of the folders. A failure is logged: the next check does again what it lost (every email
    is cleaned before being stored)."""
    try:
        await crud_settings.store(_get_db_key(cat.agent_key), state)
    except Exception as e:
        log.error(f"[EmailMonitor] Error saving the state of the mailbox: {e}")


def _folder_state(raw: Any) -> Dict[str, Any]:
    """The state of a folder: its UIDVALIDITY, the UIDs done (stored, or given up) and the failed attempts of the
    others; none yet (the first check), or the state of the previous version, is an empty folder."""
    if isinstance(raw, dict) and "uidvalidity" in raw:
        return raw
    return {"uidvalidity": None, "done": [], "attempts": {}}  # also the list of UIDs of the previous version


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


def _slug(text: str, limit: int) -> str:
    """``text`` as a piece of a file name: ASCII letters, digits, ``_`` and ``-`` only (the core replaces dots and
    spaces of a file name, so a name made of these is stored as it is), at most ``limit`` characters."""
    ascii_text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^A-Za-z0-9_-]+", "_", ascii_text).strip("_")[:limit].strip("_")


def _tag(settings: EmailMonitorSettings) -> str:
    return _slug(settings.memory_source_tag, 30) or "email"


def _folder_prefix(settings: EmailMonitorSettings, folder_type: str, uidvalidity: str) -> str:
    """The start of the names of the files of the emails of a folder under a UIDVALIDITY."""
    return f"{_tag(settings)}_{folder_type}_{_slug(uidvalidity, 20) or '0'}_"


def _email_headers(msg: Message) -> Dict[str, Any]:
    """The decoded headers of an email; a missing or unreadable date is now."""
    date_str = msg.get("Date", "")
    try:
        date_obj = parsedate_to_datetime(str(date_str)) if date_str else datetime.now(timezone.utc)
    except Exception:
        date_obj = datetime.now(timezone.utc)
    return {
        "subject": _decode_header_value(msg.get("Subject", "(no subject)")),
        "from": _decode_header_value(msg.get("From", "")),
        "to": _decode_header_value(msg.get("To", "")),
        "cc": _decode_header_value(msg.get("Cc", "")),
        "date": date_obj,
        "message_id": _decode_header_value(msg.get("Message-ID", "")).strip(),
    }


def _build_document(
    msg: Message,
    uid: str,
    folder_type: str,  # "inbox" or "sent"
    settings: EmailMonitorSettings,
    uidvalidity: str = "0",
) -> Document:
    """
    The body of an email as a document: its text (the headers, then the body, truncated at ``max_body_length``) and
    the metadata shared by every file of the email.
    """
    headers = _email_headers(msg)
    body = _extract_text_body(msg) or "(empty body)"
    if len(body) > settings.max_body_length:
        body = body[: settings.max_body_length] + "\n... [truncated]"

    direction = "RECEIVED" if folder_type == "inbox" else "SENT"
    page_content = (
        f"[EMAIL - {direction}]\n"
        f"Date: {headers['date'].isoformat()}\n"
        f"From: {headers['from']}\n"
        f"To: {headers['to']}\n"
        f"Cc: {headers['cc']}\n"
        f"Subject: {headers['subject']}\n"
        f"\n{body}"
    )
    metadata = {
        "email_source_tag": settings.memory_source_tag,
        "email_uid": uid,
        "email_folder": folder_type,
        "email_uidvalidity": uidvalidity,
        "email_direction": direction,
        "email_subject": headers["subject"],
        "email_from": headers["from"],
        "email_to": headers["to"],
        "email_date": headers["date"].isoformat(),
        "email_message_id": headers["message_id"],
    }
    return Document(page_content=page_content, metadata=metadata)


def _message_text(msg: Message) -> str:
    """Text of an attached email: its headers and its body (not its attachments), as this plugin reads them."""
    headers = _email_headers(msg)
    return (
        f"From: {headers['from']}\nTo: {headers['to']}\nCc: {headers['cc']}\n"
        f"Date: {headers['date'].isoformat()}\nSubject: {headers['subject']}\n\n{_extract_text_body(msg)}"
    ).strip()


def _attachment_parts(part: Message, found: List[Message]) -> None:
    """
    The attachments of ``part``, in order: the parts with an ``attachment`` disposition (an attached email is one
    attachment), and the inline leaves with a file name that are not text of the body nor an inline image of it
    (an image with a Content-ID: logos, signatures). Nothing inside an attachment, nor the bounced email of a delivery
    report; the attachments of an inline forwarded email are attachments of the email.
    """
    if part.get_content_disposition() == "attachment":
        found.append(part)
        return
    if part.is_multipart():
        for child in part.get_payload():
            if part.get_content_type() == "multipart/report" and child.get_content_maintype() == "message":
                continue
            _attachment_parts(child, found)
        return
    if part.get_content_type() in ("text/plain", "text/html") or not part.get_filename():
        return
    if part.get_content_maintype() == "image" and part.get("Content-ID"):
        return
    found.append(part)


@dataclass
class _EmailFile:
    """A file of an email for the ingestion engine: the body, or an attachment."""
    source: str  # unique name of the file, also the ``source`` of its points
    name: str  # the name shown (the file name of the attachment)
    data: bytes
    content_type: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class _EmailPlan:
    body: Document
    body_source: str
    attachments: List[_EmailFile]
    skipped: List[Dict[str, str]]
    # the names of the files of every attachment, also the skipped ones: what a crashed attempt may have stored
    # under other settings (e.g. attachments turned off after it)
    attachment_sources: List[str]

    @property
    def sources(self) -> List[str]:
        return [self.body_source] + self.attachment_sources


def _attachment_name(part: Message, index: int) -> str:
    raw = part.get_filename()
    name = _decode_header_value(raw).strip() if raw else ""
    if not name and part.get_content_type() == "message/rfc822" and part.is_multipart():
        subject = _decode_header_value(part.get_payload()[0].get("Subject", "")).strip()
        name = f"{subject}.eml" if subject else ""
    return name or f"allegato_{index}"


def _attachment_source(base: str, name: str, index: int, content_type: str, extension: str = "") -> str:
    """``<base>_<index>_<slug of the name>.<extension>``: the extension given, else the one of the name when it is
    one (letters and digits, up to 8), else the one of its type, else ``bin``; no folders of the name."""
    stem = re.split(r"[\\/]", name)[-1]
    ext = ""
    if "." in stem:
        head, tail = stem.rsplit(".", 1)
        stem = head
        if re.fullmatch(r"[A-Za-z0-9]{1,8}", tail):
            ext = tail
    ext = extension or ext
    if not ext:
        ext = (mimetypes.guess_extension(content_type or "") or ".bin").lstrip(".")
    return f"{base}_{index}_{_slug(stem, 60) or f'allegato_{index}'}.{ext.lower()}"


def _plan_email(
    msg: Message, uid: str, folder_type: str, uidvalidity: str, settings: EmailMonitorSettings, handlers: Dict,
) -> _EmailPlan:
    """The files to store for an email: its attachments to ingest (when enabled), the ones skipped with the
    reason, its body. Deterministic: the same email gives the same names, that the cleanup of a failed or crashed
    attempt relies on."""
    body = _build_document(msg, uid, folder_type, settings, uidvalidity)
    base = f"{_folder_prefix(settings, folder_type, uidvalidity)}{_slug(uid, 20)}"
    excluded = {t.strip().lower() for t in settings.attachment_excluded_types.split(",") if t.strip()}
    parts: List[Message] = []
    _attachment_parts(msg, parts)
    attachments: List[_EmailFile] = []
    skipped: List[Dict[str, str]] = []
    attachment_sources: List[str] = []
    for index, part in enumerate(parts, start=1):
        name = _attachment_name(part, index)
        if part.get_content_type() == "message/rfc822" and part.is_multipart():
            mime, content_type = "message/rfc822", "text/plain"
            data = _message_text(part.get_payload()[0]).encode("utf-8")
        else:  # (an attached multipart that is not an email has no payload of its own: empty)
            data = part.get_payload(decode=True) or b""
            mime = content_type = (guess_file_type(BytesIO(data))[0] or "") if data else ""
        source = _attachment_source(base, name, index, content_type, "txt" if mime == "message/rfc822" else "")
        attachment_sources.append(source)
        reason = None
        if not settings.ingest_attachments:
            reason = "disabled"
        elif not data.strip():  # nothing, or only blanks
            reason = "empty"
        elif len(data) > settings.attachment_max_mb * 1024 * 1024:
            reason = "too_large"
        elif mime in excluded or part.get_content_type() in excluded:  # text files are all text/plain by bytes
            reason = "excluded_type"
        elif not content_type or content_type not in handlers:
            reason = "unsupported_type"
        elif len(attachments) >= settings.attachment_max_count:
            reason = "too_many"
        if reason:
            skipped.append({"name": name, "reason": reason})
            continue
        attachments.append(_EmailFile(
            source=source, name=name, data=data, content_type=content_type,
            metadata=body.metadata | {"email_attachment_filename": name, "email_attachment_index": index,
                                      "email_attachment_mime": mime},
        ))
    return _EmailPlan(body=body, body_source=f"{base}.txt", attachments=attachments, skipped=skipped,
                      attachment_sources=attachment_sources)


def _email_filter(folder_type: str, uidvalidity: str, uid: str | None = None) -> Dict:
    """The points of the emails of a folder (or of one email), whatever the tag they were stored with."""
    flt = {"email_folder": folder_type, "email_uidvalidity": uidvalidity}
    return flt | ({"email_uid": uid} if uid is not None else {})


async def _delete_file(cat: CheshireCat, engine: Any, name: str) -> None:
    """Delete a file of an email as the file manager of the core does (``DELETE /files``): the uploaded file, then what
    the ingestion of the core produced for it (its points, its kept chunks, its ingestion state), the plugins told.
    Without its state and chunks, neither the automatic resume nor a change of embedder can bring it back. It waits
    for a worker ingesting it (e.g. the resume of a crashed attempt) to end."""
    async def remove_file() -> None:
        cat.file_manager.remove_file(os.path.join(cat.agent_key, name))
        await cat.plugin_manager.execute_hook("before_file_manager_file_delete", name, "agent", caller=cat)

    await engine.delete_source(cat, name, before=remove_file)
    await cat.plugin_manager.execute_hook("after_file_manager_file_deleted", name, "agent", caller=cat)


async def _stored_sources(cat: CheshireCat, prefix: str) -> List[str]:
    """The names of the files of the agent starting with ``prefix``, stored or with an ingestion state (a crash may
    leave a state without its file); only when their names cannot be computed: it lists every file of the agent."""
    names = {f.name for f in cat.file_manager.list_files(cat.agent_key)}
    names |= {row["source"] for row in await crud_ingestions.list_statuses(cat.agent_key, None)}
    return sorted(n for n in names if n.startswith(prefix))


async def _purge(
    cat: CheshireCat, engine: Any, metadata: Dict, sources: List[str] = (), existing: List[str] = (),
) -> None:
    """Delete the files with points matching ``metadata``, the files ``existing`` (found by ``_stored_sources``), and
    the files named ``sources`` that exist: the ones with an ingestion state (a crash or a failed parsing leaves a file
    without points, a crash before the file is kept a state only). The core creates the state of a file before keeping
    anything of it and removes it last: a name without state nor points has nothing to delete, so a new email costs
    no deletion."""
    points, _ = await cat.vector_memory_handler.get_all_tenant_points(DECLARATIVE, metadata=metadata,
                                                                      with_vectors=False)
    names = {p.payload.get("metadata", {}).get("source") for p in points} | set(existing)
    for name in set(sources) - names:
        if await crud_ingestions.get_status(cat.agent_key, None, name) is not None:
            names.add(name)
    for name in sorted(n for n in names if n):
        await _delete_file(cat, engine, name)


async def _ingest(cat: CheshireCat, engine: Any, file: _EmailFile) -> bool:
    """Ingest a file through the RabbitHole of the core, as an upload is. It logs and swallows the errors of the
    ingestion (a file it cannot parse, an embedder down; None when it was canceled): the file is stored when the
    embedding phase of its ingestion state, returned, is completed, i.e. it has its points, as the core decides when it
    notifies them. A phase another plugin adds may still have failed: the error stays in the state, the core shows it
    and runs it again with a change of embedder, and it does not block the mailbox. An error of its own database
    (Redis) is raised: it ends the check."""
    row = await engine.ingest_file(cat=cat, file=BytesIO(file.data), filename=file.source, metadata=file.metadata,
                                   store_file=True, content_type=file.content_type)
    embedding = ((row or {}).get("completed_phases") or {}).get(PHASE_EMBEDDING) or {}
    return embedding.get("state") == STATE_COMPLETE


class _ServiceDown(Exception):
    """The body of an email, plain text that always parses, was not stored: the embedder or the vector DB is down."""


async def _store_email(cat: CheshireCat, engine: Any, plan: _EmailPlan, last_attempt: bool) -> bool:
    """Store the files of an email: its attachments, then its body, listing what was ingested, skipped, failed.
    Returns whether the email is done (on the last attempt the failed attachments are given up, listed in the body);
    when it is not, the caller deletes all of it. A body not stored raises ``_ServiceDown``, nothing of the email
    left: the engine swallows the errors of the ingestion, the body is what tells a service down from a file nobody
    can read."""
    ingested, failed = [], []
    for file in plan.attachments:
        (ingested if await _ingest(cat, engine, file) else failed).append(file)
    body = _EmailFile(
        source=plan.body_source, name=plan.body_source, data=plan.body.page_content.encode("utf-8"),
        content_type="text/plain",
        metadata=plan.body.metadata | {
            "email_attachments_ingested": [f.name for f in ingested],
            "email_attachments_skipped": plan.skipped,
            "email_attachments_failed": [f.name for f in failed],
        },
    )
    if not await _ingest(cat, engine, body):
        meta = plan.body.metadata
        await _purge(
            cat,
            engine,
            _email_filter(meta["email_folder"], meta["email_uidvalidity"], meta["email_uid"]),
            plan.sources,
        )
        raise _ServiceDown(f"'{body.source}' not stored: the embedder or the vector DB is down")
    if failed and not last_attempt:
        return False
    for file in failed:  # kept by the engine (its file, its state, maybe its chunks), without points
        await _delete_file(cat, engine, file.source)
    return True


def _fetch(conn: imaplib.IMAP4, uid: str) -> bytes | None:
    status, data = conn.uid("FETCH", uid, "(RFC822)")
    if status != "OK" or not data or not isinstance(data[0], tuple) or not isinstance(data[0][1], bytes):
        return None
    return data[0][1]


async def _process_email(
    conn: imaplib.IMAP4, cat: CheshireCat, engine: Any, settings: EmailMonitorSettings, handlers: Dict,
    folder_type: str, uidvalidity: str, uid: str, states: Dict[str, Dict[str, Any]],
) -> None:
    """
    One attempt at an email: done (stored, or given up after ``MAX_ATTEMPTS``) or one more failed attempt, with
    nothing of it left in memory. Whatever was left by a crashed attempt is deleted first.

    The attempt is counted (and saved) before it starts: an email that kills the process (e.g. a malformed
    attachment that crashes the parser) is given up after ``MAX_ATTEMPTS`` crashes. A service down (the IMAP server, the embedder, the vector DB) is not
    the fault of the email: the count is restored and the error ends the check.
    """
    state = states[folder_type]
    previous = state["attempts"].get(uid, 0)
    email_filter = _email_filter(folder_type, uidvalidity, uid)
    base = f"{_folder_prefix(settings, folder_type, uidvalidity)}{_slug(uid, 20)}"
    if previous >= MAX_ATTEMPTS:  # the last attempt crashed
        log.error(f"[EmailMonitor] The email {folder_type}/{uid} failed {previous} times: it is given up")
        await _purge(
            cat, engine, email_filter, existing=[n for n in await _stored_sources(cat, base) if n[len(base)] in "._"],
        )
        state["done"].append(uid)
        state["attempts"].pop(uid, None)
        return
    attempt = previous + 1
    state["attempts"][uid] = attempt
    await _save_state(cat, states)
    try:
        plan = None
        try:
            raw = await asyncio.to_thread(_fetch, conn, uid)
            if raw is None:
                raise ValueError("the server did not return it")
            plan = _plan_email(email.message_from_bytes(raw), uid, folder_type, uidvalidity, settings, handlers)
        except _CONNECTION_LOST:
            raise
        except Exception as e:  # the email cannot be read
            log.error(f"[EmailMonitor] Error reading the email {folder_type}/{uid}: {e}")
        done = False
        if plan is not None:
            await _purge(cat, engine, email_filter, plan.sources)
            done = await _store_email(cat, engine, plan, attempt >= MAX_ATTEMPTS)
        if not done:
            if plan is not None:
                await _purge(cat, engine, email_filter, plan.sources)
            else:  # unread: the names of its files by their start
                await _purge(
                    cat,
                    engine,
                    email_filter,
                    existing=[n for n in await _stored_sources(cat, base) if n[len(base)] in "._"],
                )
    except Exception:
        if previous:
            state["attempts"][uid] = previous
        else:
            state["attempts"].pop(uid, None)
        raise
    if done or attempt >= MAX_ATTEMPTS:
        if not done:
            log.error(f"[EmailMonitor] The email {folder_type}/{uid} failed {attempt} times: it is given up")
        state["done"].append(uid)
        state["attempts"].pop(uid, None)


async def _check_folder(
    conn: imaplib.IMAP4, cat: CheshireCat, engine: Any, settings: EmailMonitorSettings, handlers: Dict,
    folder_type: str, folder: str, states: Dict[str, Dict[str, Any]], deadline: float,
) -> bool:
    """Store the new emails of a folder, oldest first, until the time is over. Returns False when the time is over.
    A new UIDVALIDITY makes the UIDs stored meaningless: the folder is deleted from memory and read again."""
    status, _ = await asyncio.to_thread(conn.select, _imap_folder(folder), readonly=True)
    if status != "OK":
        log.warning(f"[EmailMonitor] Could not select folder '{folder}'")
        return True
    uidvalidity = conn.response("UIDVALIDITY")[1][0].decode()  # in every SELECT response (RFC 3501)
    state = states[folder_type]
    if state["uidvalidity"] != uidvalidity:
        if state["uidvalidity"] is not None:
            log.warning(f"[EmailMonitor] UIDVALIDITY of '{folder}' changed: the folder is read again")
            # the files by name too: a crashed attempt may have left one without points, or a state only
            prefix = _folder_prefix(settings, folder_type, state["uidvalidity"])
            await _purge(
                cat,
                engine,
                _email_filter(folder_type, state["uidvalidity"]),
                existing=await _stored_sources(cat, prefix),
            )
        states[folder_type] = state = {"uidvalidity": uidvalidity, "done": [], "attempts": {}}
        await _save_state(cat, states)

    status, data = await asyncio.to_thread(conn.uid, "SEARCH", None, "ALL")
    if status != "OK":
        log.warning(f"[EmailMonitor] UID SEARCH failed in folder '{folder}'")
        return True
    done = set(state["done"])
    uids = sorted({u.decode() for u in (data[0] or b"").split()} - done, key=int) if data else []
    if uids:
        log.info(f"[EmailMonitor] {len(uids)} new email(s) found in '{folder}'")
    for uid in uids:
        if _clock() >= deadline:  # checked before an email only: the started one ends, no email is left half
            log.info("[EmailMonitor] Time budget over: the other emails at the next check")
            return False
        try:
            await _process_email(conn, cat, engine, settings, handlers, folder_type, uidvalidity, uid, states)
        finally:  # the count restored when a service is down
            await _save_state(cat, states)
    return True


def _imap_folder(name: str) -> str:
    """A folder name as IMAP wants it: modified UTF-7 (RFC 3501 5.1.3), quoted (imaplib sends the arguments as they
    are: a name with spaces or brackets, like Gmail's "[Gmail]/Sent Mail", is otherwise a BAD command)."""
    out: List[str] = []
    pending: List[str] = []  # the characters to encode

    def flush() -> None:
        if pending:
            encoded = base64.b64encode("".join(pending).encode("utf-16-be")).decode("ascii")
            out.append("&" + encoded.rstrip("=").replace("/", ",") + "-")
            pending.clear()

    for char in name:
        if " " <= char <= "~":
            flush()
            out.append("&-" if char == "&" else char)
        else:
            pending.append(char)
    flush()
    escaped = "".join(out).replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _connect(settings: EmailMonitorSettings) -> imaplib.IMAP4:
    """An authenticated IMAP connection; every operation gives up after ``IMAP_TIMEOUT_SECONDS``."""
    if settings.imap_use_ssl:
        conn = imaplib.IMAP4_SSL(settings.imap_host, settings.imap_port, timeout=IMAP_TIMEOUT_SECONDS)
    else:
        conn = imaplib.IMAP4(settings.imap_host, settings.imap_port, timeout=IMAP_TIMEOUT_SECONDS)
    try:
        if not settings.imap_use_ssl:
            conn.starttls()
        conn.login(settings.imap_username, settings.imap_password)
    except BaseException:
        _logout(conn)
        raise
    return conn


def _logout(conn: imaplib.IMAP4) -> None:
    try:
        conn.logout()
    except Exception:
        pass


async def _run_check(lizard: BillTheLizard, settings: EmailMonitorSettings, cat: CheshireCat) -> None:
    if not settings.imap_username or not settings.imap_password:
        log.warning("[EmailMonitor] IMAP credentials not configured – skipping.")
        return
    stored = await crud_settings.read(_get_db_key(cat.agent_key)) or {}
    states = {folder_type: _folder_state(stored.get(folder_type)) for folder_type, _ in FOLDERS}
    try:
        conn = await asyncio.to_thread(_connect, settings)
    except Exception as e:
        log.error(f"[EmailMonitor] IMAP connection failed: {e}")
        return
    try:
        engine = lizard.rabbit_hole
        handlers = await cat.file_handlers()
        deadline = _clock() + settings.cycle_budget_minutes * 60
        for folder_type, setting in FOLDERS:
            if not await _check_folder(conn, cat, engine, settings, handlers, folder_type,
                                       getattr(settings, setting), states, deadline):
                break
    except Exception as e:
        log.error(f"[EmailMonitor] Mailbox check interrupted: {e}")
    finally:
        await asyncio.to_thread(_logout, conn)


async def _check_mailbox(agent_id: str) -> None:
    """
    Store the new emails of the inbox and sent folders in the declarative memory of the agent, as files (the body
    as a text file, the attachments when enabled) ingested by the RabbitHole of the core: a change of embedder
    re-embeds them. Invoked periodically by the White Rabbit; one check at a time per agent (the lock, held until
    everything is stored).

    The White Rabbit keeps its jobs in Redis, pickled, across restarts and nodes: the job holds only the id of the
    agent (an agent is not serializable, and the settings would put the password in clear in Redis). Every check
    gets the agent and its settings of that moment (the settings of the agent named: the core would guess it from the
    stack); nothing is done when the agent has no cat or the plugin is not active on it (e.g. deactivated for the whole
    instance, which removes no job: a later activation on the agent keeps it).
    """
    lizard = BillTheLizard()
    job_id = _get_job_id(agent_id)
    if not lizard.white_rabbit.acquire_lock(job_id):
        return
    try:
        cat = await lizard.get_cheshire_cat(agent_id)
        if cat is None:
            return
        if PLUGIN_ID not in cat.mad_hatter.active_plugins:
            return
        settings = EmailMonitorSettings(**await cat.mad_hatter.plugins[PLUGIN_ID].load_settings(agent_id))
        log.info("[EmailMonitor] Starting mailbox check...")
        await _run_check(lizard, settings, cat)
    finally:
        lizard.white_rabbit.release_lock(job_id)


@hook(priority=1)
def before_rabbithole_stores_documents(docs: List[Document], cat) -> List[Document]:
    """The chunks of an attachment of an email say which email they come from (once: also on a re-embed)."""
    for doc in docs:
        metadata = doc.metadata or {}
        name = metadata.get("email_attachment_filename")
        if not name:
            continue
        header = (f"[ALLEGATO di '{metadata.get('email_subject', '')}' da {metadata.get('email_from', '')}, "
                  f"{metadata.get('email_date', '')}] {name}\n")
        if not doc.page_content.startswith(header):
            doc.page_content = header + doc.page_content
    return docs


def _schedule(agent_id: str, settings: Dict[str, Any] | None) -> None:
    """Schedule the periodic check of the agent every **Poll Interval** minutes of ``settings``, unless it is scheduled
    already (by another node, or before). The job holds only the id of the agent (see ``_check_mailbox``). Settings
    that are not valid schedule nothing: the error is logged."""
    try:
        interval_minutes = EmailMonitorSettings(**(settings or {})).poll_interval_minutes
        white_rabbit = BillTheLizard().white_rabbit
        if white_rabbit.get_job(_get_job_id(agent_id)):
            return
        white_rabbit.schedule_interval_job(
            job=_check_mailbox, job_id=_get_job_id(agent_id), minutes=interval_minutes, agent_id=agent_id,
        )
        log.info(f"[EmailMonitor] Mailbox check of agent {agent_id} scheduled every {interval_minutes} minute(s)")
    except Exception as e:
        log.error(f"[EmailMonitor] Unable to schedule the mailbox check of agent {agent_id}: {e}")


def _unschedule(agent_id: str) -> None:
    """Remove the job of the agent. A running check is not stopped: it ends by itself (within the **Check Time
    Budget**), and its lock keeps the checks of the agent one at a time."""
    white_rabbit = BillTheLizard().white_rabbit
    if white_rabbit.get_job(_get_job_id(agent_id)):
        white_rabbit.remove_job(_get_job_id(agent_id))


@hook(priority=0)
async def after_lizard_bootstrap(lizard) -> None:
    """At startup, align the jobs with the agents having the plugin active: the White Rabbit removes the jobs a node
    scheduled when that node shuts down (it is created by its own ``after_lizard_bootstrap``, with a higher priority).
    The failure of an agent does not stop the others."""
    if getattr(lizard, "white_rabbit", None) is None:
        return
    for agent_id in await crud_agents.get_agents_main_keys():
        try:
            if PLUGIN_ID in (await crud_plugins.get_active_plugins_from_db(agent_id) or []):
                _schedule(agent_id, await lizard.plugin_manager.plugins[PLUGIN_ID].load_settings(agent_id))
            else:
                _unschedule(agent_id)
        except Exception as e:
            log.error(f"[EmailMonitor] Unable to align the mailbox check of agent {agent_id}: {e}")


@hook(priority=1)
async def after_plugin_toggling_on_agent(plugin_id: str, cat: CheshireCat) -> None:
    """Schedule the periodic mailbox check when the plugin is activated on the agent, remove it when deactivated."""
    if plugin_id != PLUGIN_ID:
        return
    if plugin_id in cat.mad_hatter.active_plugins:
        _schedule(cat.agent_key, await cat.mad_hatter.plugins[PLUGIN_ID].load_settings(cat.agent_key))
    else:
        _unschedule(cat.agent_key)


@hook(priority=0)
async def after_plugin_settings_update(plugin_id: str, settings: Dict[str, Any], cat: CheshireCat) -> None:
    """The settings saved: the job is scheduled again with their interval (every check reads the settings of its
    moment). Nothing when the plugin is not active on the agent."""
    if plugin_id != PLUGIN_ID or plugin_id not in cat.mad_hatter.active_plugins:
        return
    _unschedule(cat.agent_key)
    _schedule(cat.agent_key, settings)


@hook(priority=0)
async def after_cheshire_cat_destroy(agent_id: str, cat: CheshireCat) -> None:
    """The agent is destroyed: its check is not scheduled anymore, and the state of its folders, out of the keys the
    core destroys, goes too (an agent created again with the same id would skip the emails it remembers). The state
    goes once a running check, which saves it, has ended: its lock is free (on any node), or after
    DESTROY_WAIT_SECONDS (logged)."""
    _unschedule(agent_id)
    white_rabbit = BillTheLizard().white_rabbit
    job_id = _get_job_id(agent_id)
    waited = 0.0
    while not (locked := white_rabbit.acquire_lock(job_id)) and waited < DESTROY_WAIT_SECONDS:
        await asyncio.sleep(JOB_POLL_SECONDS)  # asynchronous: the instance keeps serving the other requests
        waited += JOB_POLL_SECONDS
    try:
        if not locked:
            log.warning(f"[EmailMonitor] agent {agent_id}: its check still runs after {DESTROY_WAIT_SECONDS}s, "
                        "the state of its folders is deleted anyway")
        await crud_settings.delete(_get_db_key(agent_id))
    finally:
        if locked:
            white_rabbit.release_lock(job_id)
