"""The mailbox check, end to end on fakes of the IMAP server and of the Cat: every email is stored as files (its body
as a .txt, its attachments when enabled) through the RabbitHole of the core, so that a change of embedder
re-embeds them; per-email attempts, cleanup, UIDVALIDITY, time budget, lock.

Run from the core root, with the secrets the core requires (any value for the tests):
``CAT_CRYPTO_KEY=test CAT_CRYPTO_SALT=test python -m pytest cat/plugins/ccat-email-monitoring/tests``.

The Cat imports every ``.py`` file of the plugin, tests included: at import time this module needs only the stdlib,
the Cat, the plugin and hypothesis are loaded in ``setUpClass`` and in the tests.
"""
import asyncio
import copy
import json
import os
import re
import sys
import unittest
from email.message import EmailMessage
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

PLUGIN_DIR = Path(__file__).resolve().parents[1]

MODULE = None
HOOKS = {}

AGENT = "agent"
PDF = b"%PDF-1.4\n%\xe2\xe3\xcf\xd3\n1 0 obj\n<<>>\nendobj\ntrailer\n<<>>\n%%EOF\n"
PNG = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00\x1f\x15\xc4\x89"
BINARY = bytes(range(256)) * 4  # not a known type, not UTF-8 text
HANDLERS = {"text/plain": object(), "application/pdf": object(), "text/html": object()}
SECONDS_PER_FILE = 60  # the fake engine takes a minute per file


def _load():
    """Loads the plugin once, as the Cat loads it: whatever the name of its folder."""
    global MODULE
    if MODULE is not None:
        return
    from cat.looking_glass.mad_hatter.plugin import Plugin

    plugin = Plugin(str(PLUGIN_DIR))
    plugin._load_decorated_functions()
    HOOKS.update({h.name: h.function for h in plugin.hooks})
    MODULE = sys.modules[HOOKS["after_plugin_toggling_on_agent"].__module__]


class Crash(BaseException):
    """The process dies: nothing of the plugin runs after it (no except, no cleanup of the email)."""


def _sanitize(name):
    """The file name the core stores (``RabbitHole._resolve_source_bytes``): dots and spaces of the base become _."""
    if "." not in name:
        return name
    base, ext = name.rsplit(".", 1)
    return re.sub(r"[.\s]+", "_", base) + "." + ext


# ---------------------------------------------------------------------------------------------------------------------
# fakes
# ---------------------------------------------------------------------------------------------------------------------

class Folder:
    def __init__(self, uidvalidity=1):
        self.uidvalidity = uidvalidity
        self.messages = {}  # uid -> raw bytes
        self.next_uid = 1

    def append(self, raw):
        uid = self.next_uid
        self.next_uid += 1
        self.messages[uid] = raw
        return uid

    def renumber(self):
        """A new UIDVALIDITY: the same messages under new UIDs."""
        self.uidvalidity += 1
        messages = [self.messages[uid] for uid in sorted(self.messages)]
        self.messages, self.next_uid = {}, 1
        for raw in messages:
            self.append(raw)


class Server:
    """The IMAP server: folders and faults."""

    def __init__(self):
        self.folders = {"INBOX": Folder(), "Sent": Folder(uidvalidity=7)}
        self.fetch_faults = {}  # (folder, uid) -> remaining failing fetches
        self.hang = False  # every operation times out
        self.connections = []

    def client(self, host, port, timeout=None):
        if self.hang:
            raise TimeoutError("connect timed out")
        conn = Client(self, timeout)
        self.connections.append(conn)
        return conn


class Client:
    def __init__(self, server, timeout):
        self.server = server
        self.timeout = timeout
        self.folder = None
        self.untagged = {}
        self.logged_out = False

    def _check(self):
        if self.server.hang:
            raise TimeoutError("timed out")

    def starttls(self):
        self._check()

    def login(self, user, password):
        self._check()
        if password != "p":
            raise imaplib_error("AUTHENTICATIONFAILED")
        return "OK", [b"logged in"]

    def select(self, name, readonly=False):
        """imaplib sends the name as it is: a name with spaces or brackets not quoted is a BAD command."""
        self._check()
        if name.startswith('"') and name.endswith('"') and len(name) > 1:
            name = name[1:-1].replace('\\"', '"').replace("\\\\", "\\")
        elif re.search(r'[\s()\[\]{%*"\\]|[^\x00-\x7f]', name):
            raise imaplib_error(f"BAD Could not parse command: EXAMINE {name}")
        folder = self.server.folders.get(name)
        if folder is None:
            return "NO", [b"no such folder"]
        self.folder = name
        self.untagged = {"UIDVALIDITY": [str(folder.uidvalidity).encode()]}
        return "OK", [str(len(folder.messages)).encode()]

    def response(self, code):
        return code, self.untagged.pop(code, [None])

    def uid(self, command, *args):
        self._check()
        folder = self.server.folders[self.folder]
        if command == "SEARCH":
            return "OK", [b" ".join(str(u).encode() for u in sorted(folder.messages))]
        if command == "FETCH":
            uid = int(args[0])
            key = (self.folder, uid)
            if self.server.fetch_faults.get(key, 0) > 0:
                self.server.fetch_faults[key] -= 1
                return "NO", [None]
            raw = folder.messages.get(uid)
            if raw is None:
                return "OK", [None]
            return "OK", [(f"{uid} (RFC822 {{{len(raw)}}}".encode(), raw), b")"]
        raise AssertionError(command)

    def logout(self):
        self.logged_out = True
        self._check()  # a dead connection fails its logout too
        return "BYE", [b""]


def imaplib_error(message):
    import imaplib

    return imaplib.IMAP4.error(message)


class VectorDB:
    def __init__(self):
        self.points = []  # {"id", "payload": {"page_content", "metadata"}}
        self.next_id = 0
        self.down = False  # unreachable: every call fails

    @staticmethod
    def _matches(point, metadata):
        stored = point["payload"]["metadata"]
        return all(stored.get(k) == v for k, v in (metadata or {}).items())

    async def get_all_tenant_points(self, collection_name, limit=None, offset=None, metadata=None, with_vectors=True):
        assert collection_name == "declarative"
        if self.down:
            raise RuntimeError("vector db unreachable")  # e.g. grpc AioRpcError: not an OSError
        found = [SimpleNamespace(id=p["id"], payload=copy.deepcopy(p["payload"]))
                 for p in self.points if self._matches(p, metadata)]
        return (found[:limit] if limit else found), None

    async def delete_tenant_points(self, collection_name, metadata=None):
        assert collection_name == "declarative"
        if self.down:
            raise RuntimeError("vector db unreachable")
        self.points = [p for p in self.points if not self._matches(p, metadata)]

    def add(self, page_content, metadata):
        self.next_id += 1
        self.points.append({"id": self.next_id, "payload": {"page_content": page_content, "metadata": metadata}})

    def delete_source(self, source):
        self.points = [p for p in self.points if p["payload"]["metadata"]["source"] != source]

    def sources(self):
        return [p["payload"]["metadata"]["source"] for p in self.points]


class FileManager:
    def __init__(self):
        self.files = {}

    def remove_file(self, file_path):
        return self.files.pop(file_path, None) is not None

    def list_files(self, remote_root_dir):
        return [SimpleNamespace(name=os.path.basename(path)) for path in self.files
                if os.path.dirname(path) == remote_root_dir]

    def read_file(self, remote_filename, remote_root_dir=None):
        return self.files.get(os.path.join(remote_root_dir or "", remote_filename))


class Engine:
    """The RabbitHole of the core (``lizard.rabbit_hole``), synchronous, with its registry of the ingestion states
    (``crud_ingestions``). An ingestion creates the row of the source, keeps its file, then runs its phases: the
    parsing (the chunks, with ``before_rabbithole_stores_documents`` applied, kept in the storage) and the embedding (the
    points of the source replaced by the new ones, one per file here). It swallows its errors and returns the final
    row, as the core does. Faults of an ingestion: "crash_early" (the container restarts with the row only),
    "crash" (the file kept, not parsed), "crash_late" (the chunks kept, not embedded), "unreadable" (the parsing
    fails), "zero" (the embedding fails: the embedder is down; the chunks stay), "raises" (its database, Redis, is
    down: the error is raised), "canceled" (None: the agent was deleted meanwhile), "other_phase" (a phase another
    plugin adds fails after the embedding: the points stay, the state is in error). ``delete_fails`` makes the next
    deletion fail after its ``before`` (the vector DB down): the row stays ``deleting``, the uploaded file is gone."""

    IN_FLIGHT = ("uploaded", "processing", "deleting")

    def __init__(self, harness):
        self.harness = harness
        self.faults = {}  # source -> list of faults, consumed one per ingestion ("ok" passes)
        self.queue = []  # faults of the next ingestions, whatever their file
        self.broken = {}  # source -> fault of every ingestion of it
        self.break_next = None  # the next attachment ingested gets broken with this fault
        self.calls = []  # (source, clock at the start)
        self.rows = {}  # source -> row of the registry (the knowledge base of the agent)
        self.chunks = {}  # source -> the kept chunks (``.rabbithole``)
        self.delete_fails = False
        self.raised = False  # an error was raised to the plugin (Redis or the vector DB down)

    # -- the RabbitHole --
    async def ingest_file(self, cat, file, metadata, filename=None, store_file=True, content_type=None):
        h = self.harness
        source = _sanitize(filename)
        self.calls.append((source, h.clock))
        if h.in_check:
            h.check_lock_held()
        data = file.read()
        h.clock += SECONDS_PER_FILE
        # a new upload starts the lifecycle of the source again
        self.rows[source] = {
            "source": source,
            "chat_id": None,
            "status": "uploaded",
            "metadata": dict(metadata or {}),
            "content_type": content_type,
            "completed_phases": {},
            "error": None,
        }
        if self.break_next is not None and "email_attachment_filename" in (metadata or {}):
            self.broken[source], self.break_next = self.break_next, None
        faults = self.faults.get(source) or []
        fault = faults.pop(0) if faults else self.queue.pop(0) if self.queue else self.broken.get(source, "ok")
        if fault == "crash_early":
            raise Crash()
        if store_file:
            h.files.files[os.path.join(AGENT, source)] = data
        self.rows[source]["status"] = "processing"
        if fault == "crash":
            raise Crash()
        if fault == "raises":
            self.raised = True
            raise ConnectionError("redis unreachable")
        if fault == "canceled":  # what the phases produced is removed with the row; the uploaded file stays
            self._remove(source)
            return None
        self._run_phases(cat, source, data, fault)
        return copy.deepcopy(self.rows[source])

    def _run_phases(self, cat, source, data, fault="ok"):
        from langchain_core.documents import Document

        h, row = self.harness, self.rows[source]
        if self._state(row, "parsing_chunking") != "complete":
            if data is None:
                return self._fail(source, "parsing_chunking", "the source was not kept")
            if fault == "unreadable" or row["content_type"] not in h.handlers:
                return self._fail(source, "parsing_chunking", "no valid chunks")
            text = data.decode("utf-8") if row["content_type"] == "text/plain" else f"<{row['content_type']} {source}>"
            docs = [Document(page_content=text, metadata=dict(row["metadata"]) | {"source": source, "when": h.clock})]
            self.chunks[source] = HOOKS["before_rabbithole_stores_documents"](docs, cat)
            row["completed_phases"]["parsing_chunking"] = {"state": "complete"}
            row["completed_phases"].pop("embedding", None)
            if fault == "crash_late":
                raise Crash()
        if fault == "zero":
            return self._fail(source, "embedding", "the embedder is down")
        h.db.delete_source(source)  # the points of the source replaced by the new ones
        for doc in self.chunks[source]:
            h.db.add(doc.page_content, dict(doc.metadata))
        row["completed_phases"]["embedding"] = {"state": "complete", "processed": len(self.chunks[source])}
        if fault == "other_phase":
            return self._fail(source, "other_plugin_phase", "a phase of another plugin failed")
        row["completed_phases"]["other_plugin_phase"] = {"state": "complete"}
        row.update(status="completed", error=None)

    @staticmethod
    def _state(row, phase):
        return (row["completed_phases"].get(phase) or {}).get("state")

    def _fail(self, source, phase, reason):
        self.rows[source]["completed_phases"][phase] = {"state": "error", "reason": reason}
        self.rows[source].update(status="error", error=reason)

    async def delete_source(self, cat, name, chat_id=None, wait=None, before=None):
        """Waits for a worker of the source (none here: synchronous), runs ``before``, then removes what the phases
        produced (the points, the chunks) and the row; the uploaded file is left to ``before``."""
        h = self.harness
        if h.in_check:
            h.check_lock_held()
        result = await before() if before is not None else None
        if name in self.rows:
            self.rows[name]["status"] = "deleting"
        if self.delete_fails:
            self.delete_fails, self.raised = False, True
            raise RuntimeError("vector db unreachable")
        await h.db.delete_tenant_points("declarative", {"source": name})
        self._remove(name)
        return result

    def _remove(self, source):
        self.harness.db.delete_source(source)
        self.chunks.pop(source, None)
        self.rows.pop(source, None)

    # -- the registry (``cat.db.cruds.ingestions``) --
    async def get_status(self, agent_id, chat_id, source):
        assert (agent_id, chat_id) == (AGENT, None), "the knowledge base of the agent"
        return copy.deepcopy(self.rows.get(source))

    async def list_statuses(self, agent_id, chat_id=None):
        assert (agent_id, chat_id) == (AGENT, None), "the knowledge base of the agent"
        return [copy.deepcopy(row) for row in self.rows.values()]

    # -- the core, outside the plugin --
    def resume(self):
        """The automatic resume: the sources in flight (their worker died) run their phases again from what was
        kept; an interrupted deletion is completed."""
        for source, row in list(self.rows.items()):
            if row["status"] in self.IN_FLIGHT:
                self._rerun(source)

    def _rerun(self, source):
        """``RabbitHole.rerun_source``: a deletion completed, else the phases not done run (a failed parsing stays)."""
        row = self.rows[source]
        if row["status"] == "deleting":
            return self._remove(source)
        if self._state(row, "parsing_chunking") == "error":
            return None
        row["status"] = "processing"
        return self._run_phases(self.harness.cat, source, self.harness.files.read_file(source, AGENT))

    def change_embedder(self):
        """A change of the embedder: the collections recreated (every point dropped); the sources without a state
        ingested again from their file (``embed_stored_sources``), then every source with a state embedded again from
        its chunks, and the phases it misses, whatever its status (``rerun_source(invalidate=["embedding"])``)."""
        from cat.utils import guess_file_type

        h = self.harness
        legacy = {}
        for point in h.db.points:
            meta = point["payload"]["metadata"]
            if meta["source"] not in self.rows:
                legacy.setdefault(meta["source"], meta)
        h.db.points = []
        for source, metadata in legacy.items():
            content = h.files.read_file(source, AGENT)
            if content:
                content_type, _ = guess_file_type(BytesIO(content))
                asyncio.run(self.ingest_file(h.cat, BytesIO(content), metadata, filename=source, store_file=False,
                                             content_type=content_type))
        for source, row in list(self.rows.items()):
            row["completed_phases"].pop("embedding", None)  # invalidated: it runs again, with what it depends on
            self._rerun(source)


class Crud:
    def __init__(self):
        self.values = {}

    async def read(self, key):
        return copy.deepcopy(self.values.get(key))

    async def store(self, key, value):
        if getattr(self, "down", False):
            raise ConnectionError("redis unreachable")
        self.values[key] = json.loads(json.dumps(value))  # what the database keeps


class WhiteRabbit:
    def __init__(self):
        self.locks = set()
        self.acquired = 0

    def acquire_lock(self, event):
        if event in self.locks:
            return False
        self.locks.add(event)
        self.acquired += 1
        return True

    def release_lock(self, event):
        self.locks.discard(event)


class Harness:
    """The Cat, the scheduler and the IMAP server, faked; ``cycle()`` runs one mailbox check."""

    def __init__(self, **settings):
        _load()
        self.server = Server()
        self.db = VectorDB()
        self.files = FileManager()
        self.crud = Crud()
        self.rabbit = WhiteRabbit()
        self.engine = Engine(self)
        self.handlers = dict(HANDLERS)
        self.clock = 0.0
        self.in_check = False
        self.cat = MagicMock()
        self.cat.agent_key = AGENT
        self.cat.vector_memory_handler = self.db
        self.cat.file_manager = self.files

        async def file_handlers():
            return dict(self.handlers)

        self.cat.file_handlers = file_handlers
        self.hooks = []  # (hook, *args) fired by the plugin

        async def execute_hook(name, *args, caller=None):
            self.hooks.append((name, *args))

        self.cat.plugin_manager.execute_hook = execute_hook
        # the lizard of the core: its RabbitHole is the ingestion engine (no ``ingestion()`` factory any more)
        self.lizard = MagicMock(spec=["white_rabbit", "rabbit_hole"], white_rabbit=self.rabbit, rabbit_hole=self.engine)
        self.cat.lizard = self.lizard
        values = {"imap_username": "u", "imap_password": "p", "inbox_folder": "INBOX", "sent_folder": "Sent",
                  "ingest_attachments": True, "attachment_max_mb": 1, "attachment_max_count": 20,
                  "cycle_budget_minutes": 50}
        values.update(settings)
        self.settings = MODULE.EmailMonitorSettings(**values)

    def configure(self, **settings):
        self.settings = self.settings.model_copy(update=settings)

    @property
    def job_id(self):
        return f"ccat_email_monitoring:{AGENT}"

    def check_lock_held(self):
        assert self.job_id in self.rabbit.locks, "the lock is held while the emails are stored"

    def state(self):
        return self.crud.values.get(f"plugins:{self.job_id}:seen")

    def cycle(self):
        with patch.object(MODULE, "BillTheLizard", return_value=self.lizard), \
                patch.object(MODULE, "crud_settings", self.crud), \
                patch.object(MODULE, "crud_ingestions", self.engine), \
                patch.object(MODULE, "_clock", lambda: self.clock), \
                patch.object(MODULE.imaplib, "IMAP4_SSL", self.server.client), \
                patch.object(MODULE.imaplib, "IMAP4", self.server.client):
            self.in_check = True
            try:
                asyncio.run(MODULE._check_mailbox(self.settings, self.cat))
            finally:
                self.in_check = False

    def reembed(self):
        """A change of the embedder (see ``Engine.change_embedder``)."""
        self.engine.change_embedder()

    def resume(self):
        """A pass of the automatic resume of the core (see ``Engine.resume``)."""
        self.engine.resume()

    def points_of(self, folder, uid, **more):
        meta = {"email_folder": folder, "email_uid": str(uid)} | more
        return [p["payload"] for p in self.db.points if VectorDB._matches(p, meta)]


# ---------------------------------------------------------------------------------------------------------------------
# emails
# ---------------------------------------------------------------------------------------------------------------------

def attachment(kind, name=None, size=None):
    """(kind, file name, bytes) of an attachment of the model."""
    data = {
        "txt": "nota di testo è\n".encode("utf-8"),
        "pdf": PDF,
        "bin": BINARY,
        "png": PNG,
        "empty": b"",
        "big": PDF + b"0" * (size or 1_100_000),
    }.get(kind)
    return kind, name, data


def build_email(subject="Oggetto", body="Testo del corpo", attachments=(), sender="a@b.it", html=None):
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = "c@d.it"
    msg["Date"] = "Tue, 03 Nov 2026 10:00:00 +0100"
    msg["Message-ID"] = f"<{abs(hash((subject, body)))}@b.it>"
    msg.set_content(body)
    if html:
        msg.add_alternative(html, subtype="html")
    for kind, name, data in attachments:
        if kind == "eml":
            inner = EmailMessage()
            inner["Subject"] = "Inoltrata"
            inner["From"] = "x@y.it"
            inner.set_content("TESTO-DELLA-ALLEGATA")
            inner.add_attachment(PDF, maintype="application", subtype="pdf", filename="dentro.pdf")
            msg.add_attachment(inner)
            continue
        if kind == "png_cid":  # an inline image of the HTML (a logo): not an attachment
            if msg.get_content_type() != "multipart/mixed":
                msg.make_mixed()
            msg.attach(_inline_image())
            continue
        maintype, subtype = {"txt": ("text", "plain"), "pdf": ("application", "pdf"), "png": ("image", "png"),
                             "empty": ("text", "plain"), "big": ("application", "pdf"),
                             "bin": ("application", "octet-stream")}[kind]
        if maintype == "text":
            msg.add_attachment(data.decode("utf-8"), subtype=subtype, filename=name)
        else:
            msg.add_attachment(data, maintype=maintype, subtype=subtype, filename=name)
    return msg.as_bytes()


def _inline_image():
    part = EmailMessage()
    part.set_content(PNG, maintype="image", subtype="png", disposition="inline", filename="image001.png")
    part["Content-ID"] = "<logo@b.it>"
    return part


# ---------------------------------------------------------------------------------------------------------------------
# deterministic tests
# ---------------------------------------------------------------------------------------------------------------------

class TestMailboxCycle(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _load()

    def test_the_body_is_stored_as_a_text_file_with_its_metadata(self):
        h = Harness(ingest_attachments=False)
        uid = h.server.folders["INBOX"].append(build_email(attachments=[attachment("pdf", "fattura.pdf")]))
        h.cycle()
        source = f"email_monitor_inbox_1_{uid}.txt"
        self.assertEqual(h.db.sources(), [source])
        self.assertEqual(list(h.files.files), [f"{AGENT}/{source}"])
        meta = h.db.points[0]["payload"]["metadata"]
        self.assertEqual({k: meta[k] for k in ("email_source_tag", "email_uid", "email_folder", "email_uidvalidity",
                                               "email_direction", "email_subject", "email_from", "email_to")},
                         {"email_source_tag": "email_monitor", "email_uid": str(uid), "email_folder": "inbox",
                          "email_uidvalidity": "1", "email_direction": "RECEIVED", "email_subject": "Oggetto",
                          "email_from": "a@b.it", "email_to": "c@d.it"})
        self.assertTrue(meta["email_message_id"].endswith("@b.it>"))
        self.assertEqual(meta["email_attachments_skipped"], [{"name": "fattura.pdf", "reason": "disabled"}])
        self.assertEqual((meta["email_attachments_ingested"], meta["email_attachments_failed"]), ([], []))
        text = h.db.points[0]["payload"]["page_content"]
        self.assertTrue(text.startswith("[EMAIL - RECEIVED]\nDate: 2026-11-03T10:00:00+01:00\nFrom: a@b.it\n"), text)
        self.assertIn("Testo del corpo", text)
        self.assertEqual(h.state()["inbox"]["done"], [str(uid)])
        self.assertEqual(h.rabbit.locks, set())

    def test_attachments_are_stored_with_the_email_metadata_and_a_header(self):
        h = Harness()
        uid = h.server.folders["Sent"].append(build_email(
            subject="Preventivo", attachments=[attachment("pdf", "Fattura è 2024.PDF"), attachment("txt", "n.txt")]
        ))
        h.cycle()
        base = f"email_monitor_sent_7_{uid}"
        self.assertEqual(sorted(h.db.sources()),
                         sorted([f"{base}.txt", f"{base}_1_Fattura_e_2024.pdf", f"{base}_2_n.txt"]))
        pdf = h.points_of("sent", uid, email_attachment_index=1)[0]
        self.assertEqual(pdf["metadata"]["email_attachment_filename"], "Fattura è 2024.PDF")
        self.assertEqual(pdf["metadata"]["email_attachment_mime"], "application/pdf")
        self.assertEqual(pdf["metadata"]["email_direction"], "SENT")
        self.assertTrue(pdf["page_content"].startswith(
            "[ALLEGATO di 'Preventivo' da a@b.it, 2026-11-03T10:00:00+01:00] Fattura è 2024.PDF\n")
        )
        body = h.points_of("sent", uid, source=f"{base}.txt")[0]["metadata"]
        self.assertEqual(body["email_attachments_ingested"], ["Fattura è 2024.PDF", "n.txt"])
        self.assertNotIn("email_attachment_filename", body)

    def test_every_reason_to_skip_an_attachment(self):
        h = Harness(attachment_max_count=2, attachment_excluded_types="text/plain, image/gif")
        uid = h.server.folders["INBOX"].append(build_email(attachments=[
            attachment("empty", "vuoto.txt"), attachment("big", "grande.pdf"), attachment("txt", "escluso.txt"),
            attachment("bin", "dati.bin"), attachment("png", "foto.png"), attachment("pdf", "uno.pdf"),
            attachment("eml"), attachment("pdf", "tre.pdf"), ("png_cid", None, None),
        ]))
        h.cycle()
        body = h.points_of("inbox", uid, email_attachments_failed=[])[0]["metadata"]
        self.assertEqual(body["email_attachments_skipped"], [
            {"name": "vuoto.txt", "reason": "empty"},
            {"name": "grande.pdf", "reason": "too_large"},
            {"name": "escluso.txt", "reason": "excluded_type"},
            {"name": "dati.bin", "reason": "unsupported_type"},
            {"name": "foto.png", "reason": "unsupported_type"},  # no reader of images
            {"name": "tre.pdf", "reason": "too_many"},
        ])
        self.assertEqual(body["email_attachments_ingested"], ["uno.pdf", "Inoltrata.eml"])
        self.assertEqual(len(h.db.points), 3)

    def test_an_attached_email_is_stored_as_its_text(self):
        """Its headers and its body, as this plugin extracts them, not its source nor its attachments: the same
        text on the first ingestion and on the re-embed (the core reads an .eml as text/plain)."""
        h = Harness()
        uid = h.server.folders["INBOX"].append(build_email(attachments=[attachment("eml")]))
        h.cycle()
        eml = h.points_of("inbox", uid, email_attachment_index=1)[0]
        self.assertEqual(eml["metadata"]["email_attachment_mime"], "message/rfc822")
        self.assertTrue(eml["metadata"]["source"].endswith("_1_Inoltrata.txt"), eml["metadata"]["source"])
        self.assertIn("Subject: Inoltrata", eml["page_content"])
        self.assertIn("TESTO-DELLA-ALLEGATA", eml["page_content"])
        self.assertNotIn("JVBER", eml["page_content"])  # base64 of the PDF inside it

    def test_file_names(self):
        h = Harness(memory_source_tag="mio tag/../x")
        uid = h.server.folders["INBOX"].append(build_email(attachments=[
            attachment("pdf", "../../etc/passwd.pdf"), attachment("pdf", None), attachment("pdf", "x" * 300 + ".pdf"),
            attachment("pdf", "doppio.pdf"), attachment("pdf", "doppio.pdf"), attachment("pdf", "senza estensione"),
            attachment("pdf", "strano.p d f"), attachment("pdf", "日本語.pdf"),
        ]))
        h.cycle()
        sources = sorted(s for s in h.db.sources() if not s.endswith(f"_{uid}.txt"))
        for source in sources:
            with self.subTest(source=source):
                self.assertRegex(source, r"^[A-Za-z0-9_-]+\.[a-z0-9]{1,8}$")
                self.assertLessEqual(len(source), 120)
        tag = "mio_tag_x"
        self.assertIn(f"{tag}_inbox_1_{uid}_1_passwd.pdf", sources)
        self.assertIn(f"{tag}_inbox_1_{uid}_2_allegato_2.pdf", sources)
        self.assertIn(f"{tag}_inbox_1_{uid}_4_doppio.pdf", sources)
        self.assertIn(f"{tag}_inbox_1_{uid}_5_doppio.pdf", sources)
        self.assertIn(f"{tag}_inbox_1_{uid}_6_senza_estensione.pdf", sources)
        self.assertIn(f"{tag}_inbox_1_{uid}_7_strano.pdf", sources)
        self.assertIn(f"{tag}_inbox_1_{uid}_8_allegato_8.pdf", sources)  # nothing left of the name
        self.assertEqual(len(sources), 8)

    def test_the_header_hook_leaves_other_documents_alone_and_does_not_repeat(self):
        from langchain_core.documents import Document

        meta = {"email_attachment_filename": "a.pdf", "email_subject": "S", "email_from": "f", "email_date": "d"}
        doc = Document(page_content="contenuto", metadata=dict(meta))
        other = Document(page_content="altro", metadata={"source": "x.pdf"})
        body = Document(page_content="corpo", metadata={"email_uid": "1"})
        hook = HOOKS["before_rabbithole_stores_documents"]
        first = hook([doc, other, body], MagicMock())
        self.assertEqual(
            [d.page_content for d in first],
            ["[ALLEGATO di 'S' da f, d] a.pdf\ncontenuto", "altro", "corpo"],
        )
        again = hook(first, MagicMock())
        self.assertEqual(again[0].page_content, "[ALLEGATO di 'S' da f, d] a.pdf\ncontenuto")

    def test_a_transient_failure_cleans_the_email_and_retries_it(self):
        h = Harness()
        uid = h.server.folders["INBOX"].append(build_email(attachments=[attachment("pdf", "a.pdf"),
                                                                        attachment("pdf", "b.pdf")]))
        base = f"email_monitor_inbox_1_{uid}"
        h.engine.faults[f"{base}_2_b.pdf"] = ["zero"]  # stored, not parsed: a file without points
        h.cycle()
        self.assertEqual((h.db.points, h.files.files), ([], {}), "no half email, no orphan file")
        self.assertEqual(h.state()["inbox"], {"uidvalidity": "1", "done": [], "attempts": {str(uid): 1}})
        h.cycle()
        self.assertEqual(len(h.db.points), 3)
        self.assertEqual(h.state()["inbox"], {"uidvalidity": "1", "done": [str(uid)], "attempts": {}})

    def test_an_attachment_failing_every_attempt_is_listed_and_the_email_kept(self):
        h = Harness()
        uid = h.server.folders["INBOX"].append(build_email(attachments=[attachment("pdf", "a.pdf"),
                                                                        attachment("pdf", "rotto.pdf")]))
        h.engine.faults[f"email_monitor_inbox_1_{uid}_2_rotto.pdf"] = ["zero"] * 3
        for _ in range(3):
            h.cycle()
        body = h.points_of("inbox", uid, email_attachments_ingested=["a.pdf"])[0]["metadata"]
        self.assertEqual(body["email_attachments_failed"], ["rotto.pdf"])
        self.assertEqual(sorted(h.db.sources()), sorted(f"email_monitor_inbox_1_{uid}{s}" for s in (".txt",
                                                                                                    "_1_a.pdf")))
        self.assertEqual(sorted(h.files.files), sorted(f"{AGENT}/{s}" for s in h.db.sources()))
        self.assertEqual(h.state()["inbox"]["done"], [str(uid)])
        h.cycle()
        # 3 files at every attempt (the body too: a body without points tells a service down from a bad file)
        self.assertEqual(len(h.engine.calls), 3 * 3, "done: never ingested again")

    def test_an_email_the_server_never_returns_is_given_up(self):
        h = Harness()
        folder = h.server.folders["INBOX"]
        bad_fetch = folder.append(build_email(subject="fetch"))
        good = folder.append(build_email(subject="buona"))
        h.server.fetch_faults[("INBOX", bad_fetch)] = 3
        for _ in range(3):
            h.cycle()
        self.assertEqual(h.db.sources(), [f"email_monitor_inbox_1_{good}.txt"])
        self.assertEqual(sorted(h.state()["inbox"]["done"], key=int), [str(bad_fetch), str(good)])
        self.assertEqual(h.state()["inbox"]["attempts"], {})

    def test_the_embedder_down_loses_no_email(self):
        """The engine swallows its errors: a body (plain text, it always parses) without points means the embedder
        or the vector DB is down: the check ends, no attempt is counted, nothing is left half stored."""
        h = Harness()
        folder = h.server.folders["INBOX"]
        first = folder.append(build_email(subject="1", attachments=[attachment("pdf", "a.pdf")]))
        second = folder.append(build_email(subject="2"))
        for _ in range(4):  # longer than MAX_ATTEMPTS checks
            h.engine.queue = ["zero"] * 10
            h.cycle()
        self.assertEqual((h.db.points, h.files.files), ([], {}))
        self.assertEqual(h.state()["inbox"], {"uidvalidity": "1", "done": [], "attempts": {}})
        h.engine.queue = []
        h.cycle()
        self.assertEqual(h.state()["inbox"]["done"], [str(first), str(second)])
        self.assertEqual(len(h.db.points), 3)

    def test_the_embedder_down_keeps_the_attempts_already_failed(self):
        h = Harness()
        uid = h.server.folders["INBOX"].append(build_email(attachments=[attachment("pdf", "a.pdf")]))
        h.engine.faults[f"email_monitor_inbox_1_{uid}_1_a.pdf"] = ["zero"]  # the attachment fails once
        h.cycle()
        self.assertEqual(h.state()["inbox"]["attempts"], {str(uid): 1})
        h.engine.queue = ["zero", "zero"]  # then the embedder is down
        h.cycle()
        self.assertEqual(h.state()["inbox"]["attempts"], {str(uid): 1})
        self.assertEqual((h.db.points, h.files.files), ([], {}))

    def test_an_email_that_kills_the_process_is_given_up_after_three_crashes(self):
        """E.g. a malformed attachment that crashes the parser: the attempt is counted before it starts, the email
        does not restart the container forever nor block the emails after it."""
        h = Harness()
        folder = h.server.folders["INBOX"]
        killer = folder.append(build_email(subject="malformato", attachments=[attachment("pdf", "rotto.pdf")]))
        good = folder.append(build_email(subject="buona"))
        h.engine.faults[f"email_monitor_inbox_1_{killer}_1_rotto.pdf"] = ["crash"] * 3
        for _ in range(3):
            with self.assertRaises(Crash):
                h.cycle()
            h.rabbit.locks.clear()
        h.cycle()
        self.assertEqual(h.state()["inbox"]["done"], [str(killer), str(good)])
        self.assertEqual(h.db.sources(), [f"email_monitor_inbox_1_{good}.txt"])
        self.assertEqual(sorted(h.files.files), [f"{AGENT}/email_monitor_inbox_1_{good}.txt"])

    def test_the_state_of_the_previous_version_starts_again(self):
        """The committed version saved {"inbox": [uids], "sent": [uids]}, also with no email."""
        h = Harness()
        h.crud.values[f"plugins:{h.job_id}:seen"] = {"inbox": ["7", "9"], "sent": []}
        uid = h.server.folders["INBOX"].append(build_email())
        h.cycle()
        self.assertEqual(h.state()["inbox"]["done"], [str(uid)])

    def test_an_excluded_type_is_matched_on_the_declared_type_too(self):
        """Text files (CSV, XML, HTML...) are recognised from their bytes as text/plain: the type the sender
        declared is matched too."""
        h = Harness(attachment_excluded_types="text/csv")
        msg = EmailMessage()
        msg["Subject"] = "dati"
        msg.set_content("corpo")
        msg.add_attachment("a;b\n1;2\n", subtype="csv", filename="dati.csv")
        msg.add_attachment("nota\n", subtype="plain", filename="nota.txt")
        uid = h.server.folders["INBOX"].append(msg.as_bytes())
        h.cycle()
        body = h.points_of("inbox", uid, email_attachments_failed=[])[0]["metadata"]
        self.assertEqual(body["email_attachments_skipped"], [{"name": "dati.csv", "reason": "excluded_type"}])
        self.assertEqual(body["email_attachments_ingested"], ["nota.txt"])

    def test_folder_names_are_quoted_and_encoded(self):
        """Regression: imaplib does not quote: EXAMINE [Gmail]/Sent Mail is a BAD command, the Sent folder of
        Gmail (the default) was never read."""
        h = Harness(sent_folder="[Gmail]/Sent Mail")
        h.server.folders["[Gmail]/Sent Mail"] = h.server.folders.pop("Sent")
        h.server.folders["[Gmail]/Sent Mail"].append(build_email())
        h.cycle()
        self.assertEqual(h.db.sources(), ["email_monitor_sent_7_1.txt"])
        for name, wire in (("INBOX", '"INBOX"'), ("[Gmail]/Sent Mail", '"[Gmail]/Sent Mail"'),
                           ("Posta inviata", '"Posta inviata"'), ('a"b\\c', '"a\\"b\\\\c"'),
                           ("Envoyés", '"Envoy&AOk-s"'), ("~peter/mail/台北/日本語", '"~peter/mail/&U,BTFw-/&ZeVnLIqe-"'),
                           ("a&b", '"a&-b"')):
            with self.subTest(name=name):
                self.assertEqual(MODULE._imap_folder(name), wire)

    def test_a_crash_is_recovered_without_duplicates(self):
        h = Harness()
        uid = h.server.folders["INBOX"].append(build_email(attachments=[attachment("pdf", "a.pdf"),
                                                                        attachment("pdf", "b.pdf")]))
        h.engine.faults[f"email_monitor_inbox_1_{uid}_2_b.pdf"] = ["crash"]
        with self.assertRaises(Crash):
            h.cycle()
        self.assertEqual(len(h.db.points), 1, "the first attachment, stored before the crash")
        h.rabbit.locks.clear()  # the TTL of the lock of the dead process expires
        h.cycle()
        self.assertEqual(
            sorted(h.db.sources()),
            sorted(f"email_monitor_inbox_1_{uid}{s}" for s in (".txt", "_1_a.pdf", "_2_b.pdf")),
        )
        self.assertEqual(sorted(h.files.files), sorted(f"{AGENT}/{s}" for s in h.db.sources()))

    def test_a_crash_then_attachments_turned_off_leaves_no_file(self):
        """The cleanup of a crashed attempt knows the files of every attachment, also the ones the new settings
        skip."""
        h = Harness()
        uid = h.server.folders["INBOX"].append(build_email(attachments=[attachment("pdf", "a.pdf")]))
        h.engine.faults[f"email_monitor_inbox_1_{uid}_1_a.pdf"] = ["crash"]  # stored, not parsed
        with self.assertRaises(Crash):
            h.cycle()
        h.rabbit.locks.clear()
        h.configure(ingest_attachments=False)
        h.cycle()
        self.assertEqual(h.db.sources(), [f"email_monitor_inbox_1_{uid}.txt"])
        self.assertEqual(sorted(h.files.files), [f"{AGENT}/email_monitor_inbox_1_{uid}.txt"])

    def test_a_crash_then_an_email_the_server_does_not_return_leaves_no_file(self):
        h = Harness()
        uid = h.server.folders["INBOX"].append(build_email())
        h.engine.faults[f"email_monitor_inbox_1_{uid}.txt"] = ["crash"]
        with self.assertRaises(Crash):
            h.cycle()
        h.rabbit.locks.clear()
        h.server.fetch_faults[("INBOX", uid)] = 3
        for _ in range(3):
            h.cycle()
        self.assertEqual((h.db.points, h.files.files), ([], {}))
        self.assertEqual(h.state()["inbox"]["done"], [str(uid)])

    def test_a_crash_then_a_new_uidvalidity_leaves_no_file(self):
        h = Harness()
        uid = h.server.folders["INBOX"].append(build_email())
        h.engine.faults[f"email_monitor_inbox_1_{uid}.txt"] = ["crash"]  # the body stored, not parsed
        with self.assertRaises(Crash):
            h.cycle()
        h.rabbit.locks.clear()
        h.server.folders["INBOX"].renumber()
        h.cycle()
        self.assertEqual(sorted(h.files.files), [f"{AGENT}/email_monitor_inbox_2_1.txt"])

    def test_a_new_uidvalidity_drops_also_the_emails_stored_under_another_tag(self):
        h = Harness(memory_source_tag="vecchio")
        h.server.folders["INBOX"].append(build_email())
        h.cycle()
        h.configure(memory_source_tag="nuovo")
        h.server.folders["INBOX"].renumber()
        h.cycle()
        self.assertEqual(h.db.sources(), ["nuovo_inbox_2_1.txt"])
        self.assertEqual(sorted(h.files.files), [f"{AGENT}/nuovo_inbox_2_1.txt"])

    def test_a_new_uidvalidity_drops_the_folder_and_reads_it_again(self):
        h = Harness()
        folder = h.server.folders["INBOX"]
        folder.append(build_email(subject="uno", attachments=[attachment("pdf", "a.pdf")]))
        folder.append(build_email(subject="due"))
        h.server.folders["Sent"].append(build_email(subject="inviata"))
        h.cycle()
        folder.renumber()
        h.cycle()
        inbox = [p["payload"]["metadata"] for p in h.db.points if p["payload"]["metadata"]["email_folder"] == "inbox"]
        self.assertEqual({m["email_uidvalidity"] for m in inbox}, {"2"})
        self.assertEqual(len(inbox), 3)
        self.assertEqual(sorted(h.files.files), sorted(f"{AGENT}/{s}" for s in h.db.sources()))
        self.assertEqual(h.state()["inbox"]["uidvalidity"], "2")
        self.assertEqual(len(h.points_of("sent", 1)), 1, "the other folder is untouched")

    def test_the_budget_is_checked_before_each_email_and_the_cycle_always_progresses(self):
        h = Harness(cycle_budget_minutes=1)
        folder = h.server.folders["INBOX"]
        first = folder.append(build_email(subject="1", attachments=[attachment("pdf", f"{i}.pdf") for i in range(3)]))
        second = folder.append(build_email(subject="2"))
        h.cycle()
        self.assertEqual(h.state()["inbox"]["done"], [str(first)], "the started email ends, the next waits")
        self.assertEqual(len(h.engine.calls), 4)
        h.cycle()
        self.assertEqual(h.state()["inbox"]["done"], [str(first), str(second)])

    def test_the_lock_is_released_on_every_exit_and_the_connection_closed(self):
        for case in ("no credentials", "connection", "timeout", "crash", "ok"):
            with self.subTest(case=case):
                h = Harness(imap_password="" if case == "no credentials" else "p")
                uid = h.server.folders["INBOX"].append(build_email())
                if case == "connection":
                    h.server.hang = True
                if case == "crash":
                    h.engine.faults[f"email_monitor_inbox_1_{uid}.txt"] = ["crash"]
                if case == "timeout":
                    h.cycle()  # connects
                    h.server.folders["INBOX"].append(build_email())

                    def hang(client, *args):
                        raise TimeoutError("timed out")

                    with patch.object(Client, "uid", hang):
                        h.cycle()
                    self.assertTrue(h.server.connections[-1].logged_out)
                elif case == "crash":
                    with self.assertRaises(Crash):
                        h.cycle()
                else:
                    h.cycle()
                self.assertEqual(h.rabbit.locks, set())
                for conn in h.server.connections:
                    self.assertTrue(conn.logged_out)
                    self.assertEqual(conn.timeout, MODULE.IMAP_TIMEOUT_SECONDS)
                if case == "no credentials":
                    self.assertEqual(h.server.connections, [])

    def test_a_running_cycle_holds_the_lock_another_one_skips(self):
        h = Harness()
        h.server.folders["INBOX"].append(build_email())
        h.rabbit.locks.add(h.job_id)
        h.cycle()
        self.assertEqual((h.db.points, h.server.connections), ([], []))
        self.assertEqual(h.rabbit.locks, {h.job_id}, "not released: it is not ours")

    def test_the_stored_emails_survive_a_change_of_embedder(self):
        h = Harness()
        h.server.folders["INBOX"].append(build_email(attachments=[attachment("pdf", "a.pdf"),
                                                                  attachment("txt", "n.txt"), attachment("eml")]))
        h.cycle()
        before = sorted((p["payload"]["metadata"]["source"], p["payload"]["page_content"]) for p in h.db.points)
        h.reembed()
        after = sorted((p["payload"]["metadata"]["source"], p["payload"]["page_content"]) for p in h.db.points)
        self.assertEqual(after, before)

    def assert_only_the_stored_files(self, h, sources):
        """Of the ingestion of the core, only the files of ``sources`` are left (completed): no state, chunk, file or
        point of anything the plugin deleted, that the automatic resume or a change of embedder would bring back."""
        sources = sorted(sources)
        self.assertEqual(sorted(h.db.sources()), sources, "points")
        self.assertEqual(sorted(h.files.files), [f"{AGENT}/{s}" for s in sources], "files")
        self.assertEqual(sorted(h.engine.rows), sources, "ingestion states")
        self.assertEqual(sorted(h.engine.chunks), sources, "kept chunks")
        self.assertEqual({r["status"] for r in h.engine.rows.values()}, {"completed"} if sources else set())

    def test_a_phase_of_another_plugin_failing_does_not_block_the_mailbox(self):
        """The file has its points (its embedding completed): it is stored, as for the core, which notifies its points;
        the error stays in its ingestion state, for the core to show and to run again with a change of embedder."""
        h = Harness()
        uid = h.server.folders["INBOX"].append(build_email(attachments=[attachment("pdf", "a.pdf")]))
        base = f"email_monitor_inbox_1_{uid}"
        h.engine.faults[f"{base}.txt"] = ["other_phase"]
        h.engine.faults[f"{base}_1_a.pdf"] = ["other_phase"]
        h.cycle()
        h.cycle()
        self.assertEqual(sorted(h.db.sources()), [f"{base}.txt", f"{base}_1_a.pdf"])
        self.assertEqual(len(h.engine.calls), 2, "stored once, not tried again")
        self.assertEqual(h.state()["inbox"], {"uidvalidity": "1", "done": [str(uid)], "attempts": {}})
        body = h.points_of("inbox", uid, source=f"{base}.txt")[0]["metadata"]
        self.assertEqual((body["email_attachments_ingested"], body["email_attachments_failed"]), (["a.pdf"], []))
        self.assertEqual({r["status"] for r in h.engine.rows.values()}, {"error"})
        h.reembed()
        self.assert_only_the_stored_files(h, [f"{base}.txt", f"{base}_1_a.pdf"])

    def test_an_attachment_given_up_does_not_come_back_with_a_new_embedder(self):
        """Its chunks were kept (the embedding failed): a state left behind would be embedded again."""
        h = Harness()
        uid = h.server.folders["INBOX"].append(build_email(attachments=[attachment("pdf", "rotto.pdf")]))
        h.engine.faults[f"email_monitor_inbox_1_{uid}_1_rotto.pdf"] = ["zero"] * 3
        for _ in range(3):
            h.cycle()
        h.reembed()
        h.resume()
        self.assert_only_the_stored_files(h, [f"email_monitor_inbox_1_{uid}.txt"])
        deleted = [args[1] for args in h.hooks if args[0] == "after_file_manager_file_deleted"]
        self.assertIn(f"email_monitor_inbox_1_{uid}_1_rotto.pdf", deleted, "the other plugins are told")

    def test_the_emails_of_an_old_uidvalidity_do_not_come_back_with_a_new_embedder(self):
        h = Harness()
        h.server.folders["INBOX"].append(build_email(attachments=[attachment("pdf", "a.pdf")]))
        h.cycle()
        h.server.folders["INBOX"].renumber()
        h.cycle()
        h.reembed()
        self.assert_only_the_stored_files(h, ["email_monitor_inbox_2_1.txt", "email_monitor_inbox_2_1_1_a.pdf"])

    def test_a_crash_resumed_by_the_core_then_retried_leaves_no_duplicate_nor_orphan(self):
        """The core resumes the files of the crashed attempt before the plugin retries the email."""
        h = Harness()
        uid = h.server.folders["INBOX"].append(build_email(
            attachments=[attachment("pdf", "a.pdf"), attachment("pdf", "b.pdf")],
        ))
        h.engine.faults[f"email_monitor_inbox_1_{uid}_2_b.pdf"] = ["crash_late"]
        with self.assertRaises(Crash):
            h.cycle()
        h.rabbit.locks.clear()
        h.resume()
        h.cycle()
        h.reembed()
        self.assert_only_the_stored_files(
            h,
            [f"email_monitor_inbox_1_{uid}{s}" for s in (".txt", "_1_a.pdf", "_2_b.pdf")],
        )

    def test_a_crash_before_the_file_is_kept_leaves_nothing_when_the_email_is_given_up(self):
        """The state of the source is the only trace of that crash: no file, no point; the resume makes it an
        error (the source was not kept)."""
        h = Harness()
        uid = h.server.folders["INBOX"].append(build_email())
        h.engine.faults[f"email_monitor_inbox_1_{uid}.txt"] = ["crash_early"] * 3
        for _ in range(3):
            with self.assertRaises(Crash):
                h.cycle()
            h.rabbit.locks.clear()
            h.resume()
        h.cycle()
        self.assertEqual(h.state()["inbox"]["done"], [str(uid)])
        self.assert_only_the_stored_files(h, [])

    def test_a_crash_before_the_file_is_kept_then_a_new_uidvalidity_leaves_nothing(self):
        h = Harness()
        h.server.folders["INBOX"].append(build_email())
        h.engine.queue = ["crash_early"]
        with self.assertRaises(Crash):
            h.cycle()
        h.rabbit.locks.clear()
        h.server.folders["INBOX"].renumber()
        h.cycle()
        self.assert_only_the_stored_files(h, ["email_monitor_inbox_2_1.txt"])

    def test_a_folder_that_does_not_exist_is_skipped(self):
        """A misconfigured folder name (e.g. the Sent folder of another provider): the other folder is stored."""
        h = Harness(sent_folder="Nessuna")
        h.server.folders["INBOX"].append(build_email())
        h.cycle()
        self.assertEqual(h.db.sources(), ["email_monitor_inbox_1_1.txt"])
        self.assertEqual(h.state()["sent"], {"uidvalidity": None, "done": [], "attempts": {}})


class TestMailboxEdges(unittest.TestCase):
    """The other outcomes of the life of a check: what the server, the network and the databases do."""

    @classmethod
    def setUpClass(cls):
        _load()

    def test_the_fake_core_has_the_signatures_of_the_core(self):
        """The fake stands for the RabbitHole (``ingest_file``, ``delete_source``: same parameters, order and
        defaults) and for the registry of the states (``get_status``, ``list_statuses``: same parameters, called
        with all of them), so a change of the core API breaks this test instead of passing unseen on the fake."""
        import inspect
        from cat.db.cruds import ingestions
        from cat.rabbit_hole import RabbitHole

        def params(fn, defaults=True):
            return [
                (p.name, p.kind) + ((p.default,) if defaults else ())
                for p in inspect.signature(fn).parameters.values()
            ]

        for name in ("ingest_file", "delete_source"):
            self.assertEqual(params(getattr(Engine, name)), params(getattr(RabbitHole, name)), name)
            self.assertTrue(inspect.iscoroutinefunction(getattr(RabbitHole, name)), name)
        for name in ("get_status", "list_statuses"):
            self.assertEqual(params(getattr(Engine, name), False)[1:], params(getattr(ingestions, name), False), name)
            self.assertTrue(inspect.iscoroutinefunction(getattr(ingestions, name)), name)

    def test_an_ingestion_canceled_is_a_file_not_stored(self):
        """``ingest_file`` returns None when the source is gone meanwhile (e.g. the agent deleted)."""
        h = Harness()
        uid = h.server.folders["INBOX"].append(build_email(attachments=[attachment("pdf", "a.pdf")]))
        h.engine.faults[f"email_monitor_inbox_1_{uid}_1_a.pdf"] = ["canceled"]
        h.cycle()
        self.assertEqual(h.state()["inbox"]["attempts"], {str(uid): 1}, "a failed attempt")
        self.assertEqual(h.db.points, [])

    def test_the_database_of_the_core_down_ends_the_check_and_the_next_one_cleans(self):
        """An error of Redis raised by ``ingest_file``, or a deletion failing halfway (left ``deleting``): not the
        fault of the email; the next check (or the resume of the core) completes the cleanup."""
        for fault in ("raises", "delete"):
            with self.subTest(fault):
                h = Harness()
                uid = h.server.folders["INBOX"].append(build_email(attachments=[attachment("pdf", "a.pdf"),
                                                                                attachment("pdf", "b.pdf")]))
                if fault == "raises":
                    h.engine.faults[f"email_monitor_inbox_1_{uid}_2_b.pdf"] = ["raises"]
                else:
                    h.engine.faults[f"email_monitor_inbox_1_{uid}_2_b.pdf"] = ["zero"] * 4  # the last attempt twice
                    h.cycle()
                    h.cycle()
                    h.engine.delete_fails = True  # the last attempt: the failed attachment is deleted
                h.cycle()
                self.assertNotIn(str(uid), h.state()["inbox"]["done"])
                self.assertEqual(h.rabbit.locks, set())
                h.cycle()
                self.assertEqual(h.state()["inbox"]["done"], [str(uid)])
                ingested = ["a.pdf", "b.pdf"] if fault == "raises" else ["a.pdf"]
                self.assertEqual(
                    h.points_of(
                        "inbox", uid, email_attachments_ingested=ingested
                    )[0]["metadata"]["email_attachments_failed"],
                    [] if fault == "raises" else ["b.pdf"],
                )
                h.reembed()
                stored = [".txt", "_1_a.pdf"] + (["_2_b.pdf"] if fault == "raises" else [])
                TestMailboxCycle.assert_only_the_stored_files(
                    self, h, [f"email_monitor_inbox_1_{uid}{s}" for s in stored]
                )

    def test_the_imap_calls_do_not_block_the_event_loop(self):
        """Regression: imaplib is blocking: a slow server (up to the 60 s timeout per operation) froze every agent
        and chat of the instance while the check waited for it."""
        import time

        h = Harness()
        h.server.folders["INBOX"].append(build_email())
        original = Client.uid

        def slow(client, command, *args):
            time.sleep(0.3)
            return original(client, command, *args)

        ticks = 0

        async def heartbeat():
            nonlocal ticks
            while True:
                await asyncio.sleep(0.01)
                ticks += 1

        async def check_with_heartbeat():
            beat = asyncio.create_task(heartbeat())
            try:
                await MODULE._check_mailbox(h.settings, h.cat)
            finally:
                beat.cancel()

        with patch.object(Client, "uid", slow), patch.object(MODULE, "BillTheLizard", return_value=h.lizard), \
                patch.object(MODULE, "crud_settings", h.crud), patch.object(MODULE, "crud_ingestions", h.engine), \
                patch.object(MODULE, "_clock", lambda: h.clock), \
                patch.object(MODULE.imaplib, "IMAP4_SSL", h.server.client):
            asyncio.run(check_with_heartbeat())
        self.assertEqual(len(h.db.points), 1)
        self.assertGreater(ticks, 30, "other coroutines run while the server answers (2 calls of 0.3 s)")

    def test_the_clock_is_monotonic(self):
        self.assertLessEqual(MODULE._clock(), MODULE._clock())

    def test_an_unreadable_date_is_now_and_a_long_body_is_truncated(self):
        h = Harness(max_body_length=100)
        raw = build_email(body="x" * 500).replace(b"Tue, 03 Nov 2026 10:00:00 +0100", b"ieri sera")
        h.server.folders["INBOX"].append(raw)
        h.cycle()
        text = h.db.points[0]["payload"]["page_content"]
        self.assertTrue(text.endswith("x" * 100 + "\n... [truncated]"))
        self.assertTrue(h.db.points[0]["payload"]["metadata"]["email_date"].startswith("20"))

    def test_the_bounced_email_of_a_delivery_report_is_not_an_attachment(self):
        h = Harness()
        bounced = EmailMessage()
        bounced["Subject"] = "originale"
        bounced.set_content("testo")
        report = EmailMessage()
        report["Subject"] = "Undelivered"
        report.set_content("Consegna non riuscita.")
        report.add_attachment(bounced, disposition="inline")
        report.replace_header(
            "Content-Type",
            report.get("Content-Type").replace("multipart/mixed", "multipart/report"),
        )
        h.server.folders["INBOX"].append(report.as_bytes())
        h.cycle()
        meta = h.db.points[0]["payload"]["metadata"]
        self.assertEqual((len(h.db.points), meta["email_attachments_skipped"]), (1, []))

    def test_an_inline_file_with_a_name_is_an_attachment_an_inline_image_is_not(self):
        h = Harness()
        msg = EmailMessage()
        msg["Subject"] = "inline"
        msg.set_content("corpo")
        msg.add_attachment(PDF, maintype="application", subtype="pdf", filename="offerta.pdf", disposition="inline")
        msg.attach(_inline_image())
        uid = h.server.folders["INBOX"].append(msg.as_bytes())
        h.cycle()
        self.assertEqual(sorted(h.db.sources()), [f"email_monitor_inbox_1_{uid}.txt",
                                                   f"email_monitor_inbox_1_{uid}_1_offerta.pdf"])

    def test_the_connection_lost_while_fetching_ends_the_check_without_an_attempt(self):
        h = Harness()
        uid = h.server.folders["INBOX"].append(build_email())

        def drop(client, command, *args):
            if command != "FETCH":
                return original(client, command, *args)
            h.server.hang = True  # the connection is dead: its logout fails too
            raise ConnectionResetError("connection reset")

        original = Client.uid
        with patch.object(Client, "uid", drop):
            h.cycle()
        self.assertEqual(h.state()["inbox"]["attempts"], {}, "not the email's fault")
        self.assertEqual(h.rabbit.locks, set())
        h.server.hang = False
        h.cycle()
        self.assertEqual(h.state()["inbox"]["done"], [str(uid)])

    def test_the_vector_db_down_ends_the_check_without_an_attempt(self):
        h = Harness()
        first = h.server.folders["INBOX"].append(build_email())
        h.server.folders["INBOX"].append(build_email(subject="seconda"))
        h.db.down = True
        h.cycle()
        self.assertEqual(h.state()["inbox"]["attempts"], {}, "not the email's fault")
        self.assertEqual(h.engine.calls, [], "the check ends at the first failure: the cleanup before storing")
        h.db.down = False
        h.cycle()
        self.assertEqual(h.state()["inbox"]["done"], [str(first), str(first + 1)])
        self.assertEqual(sorted(h.files.files), sorted(f"{AGENT}/{s}" for s in h.db.sources()))

    def test_a_search_refused_by_the_server_skips_the_folder(self):
        h = Harness()
        h.server.folders["INBOX"].append(build_email())
        h.server.folders["Sent"].append(build_email())

        def refuse(client, command, *args):
            if command == "SEARCH" and client.folder == "INBOX":
                return "NO", [b"search failed"]
            return original(client, command, *args)

        original = Client.uid
        with patch.object(Client, "uid", refuse):
            h.cycle()
        self.assertEqual(h.db.sources(), ["email_monitor_sent_7_1.txt"])

    def test_a_plain_connection_is_upgraded_with_starttls(self):
        h = Harness(imap_use_ssl=False, imap_port=143)
        h.server.folders["INBOX"].append(build_email())
        with patch.object(Client, "starttls", autospec=True) as starttls:
            h.cycle()
        starttls.assert_called_once()
        self.assertEqual(len(h.db.points), 1)

    def test_a_wrong_password_closes_the_connection(self):
        h = Harness(imap_password="sbagliata")
        h.server.folders["INBOX"].append(build_email())
        h.cycle()
        self.assertEqual(h.db.points, [])
        self.assertTrue(h.server.connections[0].logged_out)
        self.assertEqual(h.rabbit.locks, set())

    def test_the_state_not_saved_is_done_again_without_duplicates(self):
        h = Harness()
        uid = h.server.folders["INBOX"].append(build_email(attachments=[attachment("pdf", "a.pdf")]))
        h.crud.down = True
        h.cycle()
        h.crud.down = False
        h.cycle()
        self.assertEqual(sorted(h.db.sources()), sorted([f"email_monitor_inbox_1_{uid}.txt",
                                                          f"email_monitor_inbox_1_{uid}_1_a.pdf"]))
        self.assertEqual(h.state()["inbox"]["done"], [str(uid)])


# ---------------------------------------------------------------------------------------------------------------------
# stateful test
# ---------------------------------------------------------------------------------------------------------------------

KINDS = ["txt", "pdf", "bin", "png", "empty", "big", "eml"]


class TestMailboxStateMachine(unittest.TestCase):
    """Sequences of new emails, faults, cycles, crashes, new UIDVALIDITY, re-embeds and settings, with the business
    invariants checked after every step against a model of the expected outcome."""

    @classmethod
    def setUpClass(cls):
        _load()

    def test_invariants(self):
        from hypothesis import HealthCheck, settings, strategies as st
        from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

        case = self

        class Machine(RuleBasedStateMachine):
            def __init__(self):
                super().__init__()
                self.h = Harness(attachment_max_count=3)
                self.expected = {}  # (folder_type, uidvalidity, uid) -> attachment kinds and names
                # emails possibly left half stored by a crash, with their attempts then: until a cycle processes
                # them (their state changes), they may hold points and files
                self.dirty = {}

            # -- operations --
            @rule(folder=st.sampled_from(["INBOX", "Sent"]),
                  kinds=st.lists(st.sampled_from(KINDS), max_size=4),
                  names=st.lists(st.sampled_from(["a.pdf", "è.txt", None, "x y.doc", "a.pdf"]), min_size=4,
                                 max_size=4))
            def new_email(self, folder, kinds, names):
                atts = [attachment(kind, names[i] if kind != "eml" else None) for i, kind in enumerate(kinds)]
                self.h.server.folders[folder].append(build_email(subject=f"s{len(self.expected)}",
                                                                 attachments=atts))

            @rule(faults=st.lists(
                st.sampled_from(
                    ["zero", "unreadable", "crash", "crash_early", "crash_late", "raises", "other_phase", "ok", "ok",
                     "ok"]
                ),
                min_size=1,
                max_size=6,
            ))
            def engine_faults(self, faults):
                self.h.engine.queue = list(faults)

            @rule()
            def broken_file(self):  # an attachment no parser can read: it fails at every attempt
                self.h.engine.break_next = "zero"

            @rule(times=st.integers(1, 4), which=st.integers(0, 50), folder=st.sampled_from(["INBOX", "Sent"]))
            def fetch_fault(self, times, which, folder):
                uids = sorted(self.h.server.folders[folder].messages)
                if uids:
                    self.h.server.fetch_faults[(folder, uids[which % len(uids)])] = times

            @rule(minutes=st.integers(1, 5))
            def budget(self, minutes):
                self.h.configure(cycle_budget_minutes=minutes)

            @rule(on=st.booleans())
            def attachments(self, on):
                self.h.configure(ingest_attachments=on)

            @rule(folder=st.sampled_from(["INBOX", "Sent"]))
            def new_uidvalidity(self, folder):
                self.h.server.folders[folder].renumber()

            @rule()
            def cycle(self):
                try:
                    self.h.cycle()
                except Crash:
                    self._interrupted()
                    self.h.rabbit.locks.clear()  # the TTL of the lock of the dead process expires
                    return
                if self.h.engine.raised:  # a database down ended the check halfway through an email
                    self.h.engine.raised = False
                    self._interrupted()
                    return
                for key, attempts in list(self.dirty.items()):  # processed since: clean again
                    state = self._state(key[0])
                    if state.get("uidvalidity") != str(key[1]) or str(key[2]) in state["done"] \
                            or state["attempts"].get(str(key[2]), 0) != attempts:
                        if state.get("uidvalidity") is not None:
                            del self.dirty[key]

            def _interrupted(self):
                """The emails not done may hold what the interrupted attempt stored, until a cycle processes them."""
                for key in self._emails():
                    state = self._state(key[0])
                    if str(key[2]) not in state["done"]:
                        self.dirty[key] = state["attempts"].get(str(key[2]), 0)

            @rule()
            def delete_fault(self):  # the next deletion fails halfway (the vector DB down)
                self.h.engine.delete_fails = True

            @rule()
            def another_cycle(self):  # cycles are what moves the state: twice as likely
                self.cycle()

            @rule()
            def reembed(self):
                engine = self.h.engine
                saved = engine.faults, engine.queue, engine.broken, engine.break_next
                engine.faults, engine.queue, engine.broken, engine.break_next = {}, [], {}, None
                self.h.reembed()
                engine.faults, engine.queue, engine.broken, engine.break_next = saved

            @rule()
            def resume(self):
                """The automatic resume of the core, between two checks. In production it runs during a check too
                (every minute, on any pod): the lock of each source serializes it with the plugin, so a resume in the
                middle of a check is a resume before or after one of its ingestions or deletions."""
                self.h.resume()

            # -- model helpers --
            def _emails(self):
                for name, folder in self.h.server.folders.items():
                    ftype = "inbox" if name == "INBOX" else "sent"
                    for uid in folder.messages:
                        yield ftype, folder.uidvalidity, uid

            def _state(self, ftype):
                return (self.h.state() or {}).get(ftype) or {"done": [], "attempts": {}, "uidvalidity": None}

            # -- invariants --
            @invariant()
            def lock_free(self):
                case.assertEqual(self.h.rabbit.locks, set())

            @invariant()
            def one_ingestion_per_source(self):
                sources = self.h.db.sources()
                case.assertEqual(len(sources), len(set(sources)), "a source stored twice")

            def _dirty_source(self, source):
                """A file of an email left half by an interrupted attempt, or being deleted (an interrupted deletion:
                the core or the next check completes it)."""
                if (self.h.engine.rows.get(source) or {}).get("status") == "deleting":
                    return True
                match = re.match(r"email_monitor_(inbox|sent)_(\d+)_(\d+)[._]", source)
                return match is not None and (match[1], int(match[2]), int(match[3])) in self.dirty

            @invariant()
            def files_are_the_sources_of_the_points(self):
                stored = {f"{AGENT}/{s}" for s in self.h.db.sources()}
                extra = [f for f in self.h.files.files if f not in stored]
                case.assertEqual([f for f in extra if not self._dirty_source(os.path.basename(f))], [])
                case.assertEqual(
                    [f for f in stored if f not in self.h.files.files and not self._dirty_source(os.path.basename(f))],
                    [],
                )

            @invariant()
            def the_core_keeps_only_the_files_of_the_points(self):
                """No ingestion state or kept chunks of a file without points (out of the emails left half by a
                crash): the resume or a change of embedder would bring it back. A file with points has its embedding
                completed (its state may be in error for a phase of another plugin)."""
                stored = set(self.h.db.sources())
                for name in set(self.h.engine.rows) | set(self.h.engine.chunks) | stored:
                    if not self._dirty_source(name):
                        case.assertIn(name, stored, "a state or chunks without points")
                        row = self.h.engine.rows.get(name) or {"completed_phases": {}}
                        case.assertEqual(
                            Engine._state(row, "embedding"), "complete", f"{name}: points without a completed embedding"
                        )

            @invariant()
            def no_point_of_an_old_uidvalidity(self):
                current = {("inbox" if n == "INBOX" else "sent"): str(f.uidvalidity)
                           for n, f in self.h.server.folders.items()}
                stored = {ftype: (self.h.state() or {}).get(ftype, {}).get("uidvalidity") for ftype in current}
                for point in self.h.db.points:
                    meta = point["payload"]["metadata"]
                    if stored[meta["email_folder"]] == current[meta["email_folder"]]:
                        case.assertEqual(meta["email_uidvalidity"], current[meta["email_folder"]])

            @invariant()
            def emails_not_done_have_nothing_stored(self):
                for ftype, validity, uid in self._emails():
                    if (ftype, validity, uid) in self.dirty:
                        continue
                    state = self._state(ftype)
                    if state.get("uidvalidity") != str(validity):
                        continue
                    stored = [
                        p for p in self.h.db.points
                        if p["payload"]["metadata"]["email_folder"] == ftype
                           and p["payload"]["metadata"]["email_uid"] == str(uid)
                           and p["payload"]["metadata"]["email_uidvalidity"] == str(validity)
                    ]
                    if str(uid) not in state["done"]:
                        case.assertEqual(stored, [], (ftype, uid))
                        case.assertLessEqual(state["attempts"].get(str(uid), 0), MODULE.MAX_ATTEMPTS)
                        continue
                    bodies = [p for p in stored if "email_attachment_filename" not in p["payload"]["metadata"]]
                    case.assertLessEqual(len(bodies), 1)
                    if not bodies:  # excluded: nothing at all
                        case.assertEqual(stored, [])
                        continue
                    meta = bodies[0]["payload"]["metadata"]
                    names = (
                            meta["email_attachments_ingested"]
                            + meta["email_attachments_failed"]
                            + [s["name"] for s in meta["email_attachments_skipped"]]
                    )
                    attached = [
                        p["payload"]["metadata"]["email_attachment_filename"]
                        for p in stored
                        if "email_attachment_filename" in p["payload"]["metadata"]
                    ]
                    case.assertEqual(sorted(attached), sorted(meta["email_attachments_ingested"]))
                    case.assertEqual(len(names), self._attachment_count(ftype, uid))

            def _attachment_count(self, ftype, uid):
                import email as email_lib
                import email.policy

                folder = self.h.server.folders["INBOX" if ftype == "inbox" else "Sent"]
                msg = email_lib.message_from_bytes(folder.messages[uid], policy=email.policy.default)
                return sum(1 for part in msg.iter_attachments() if part.get("Content-ID") is None)

        Machine.TestCase.settings = settings(
            max_examples=200,
            stateful_step_count=30,
            deadline=None,
            suppress_health_check=[HealthCheck.too_slow],
        )
        Machine.TestCase("runTest").runTest()


if __name__ == "__main__":
    unittest.main()
