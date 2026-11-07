# 📬 Email Monitor Plugin — Grinning Cat AI

Monitors an email mailbox via IMAP and automatically stores every **incoming** (Inbox) and **outgoing** (Sent) email, and optionally its attachments, into the Cat's **declarative vector memory**.

---

## How It Works

1. The plugin connects to the IMAP server using the credentials configured in the plugin *Settings* (every IMAP
   operation gives up after 60 seconds).
2. It periodically scans (every N minutes, configurable) both the Inbox and Sent folders, oldest email first.
3. Every new email is stored as **files** through the RabbitHole of the core, as an upload is: its body as a
   text file, and its attachments when **Ingest Attachments** is on. The files are kept in the file storage of the
   agent, so a **change of embedder re-embeds them** like any uploaded file.
4. The state of each folder (its UIDVALIDITY, the UIDs done, the failed attempts) is saved in the database after
   every email: an interrupted check (e.g. a restart) resumes where it stopped, without duplicates.

A check holds a lock for its whole duration (one check at a time per agent, also across nodes) and starts no new
email after **Check Time Budget** minutes (the email it is storing is completed), so a deactivation or a settings
update, which wait for the running check, wait at most that long.

An email whose storing fails (an attachment the parser cannot read, an email the server does not return) is deleted
from memory, files included, and tried again at the next check; after 3 failed attempts it is given up (logged as an
error): its failed attachments are listed in the metadata of its body, or, when it cannot be read at all, it is
skipped. An attempt is counted before it starts, so an email that crashes the process (e.g. a malformed attachment
that kills the parser) is given up after 3 restarts too; a restart for any other reason while an email is being
stored costs that email one attempt.

A service that is down is not the fault of the email: the IMAP server or the vector database unreachable, the
embedder failing (detected as the body of the email, plain text, not being stored) end the check without counting
an attempt, and the next check tries again, so no email is lost to an outage.

When the server changes the UIDVALIDITY of a folder (its UIDs are not valid anymore), the emails of that folder are
deleted from memory and the folder is read again.

The Cat will then be able to recall emails during conversations thanks to the RAG (Retrieval-Augmented Generation) mechanism.

---

## Installation

### 1. Install the plugin

Install the plugin through the Cat's plugin manager.

### 2. Dependencies

The plugin uses Python's built-in `imaplib` module from the standard library, so **no additional installation is required**. It also depends on the **White Rabbit** core plugin for scheduled jobs.

### 3. Activate the plugin

Go to the Admin panel → **Plugins** and enable *Email Monitor*.

### 4. Configure the settings

Click the ⚙️ icon next to the plugin and fill in the fields:

| Field                   | Description                          | Example               |
|-------------------------|--------------------------------------|-----------------------|
| **IMAP Host**           | IMAP server hostname                 | `imap.gmail.com`      |
| **IMAP Port**           | IMAP port                            | `993` (SSL)           |
| **IMAP Use SSL**        | Enable SSL/TLS                       | `true`                |
| **IMAP Username**       | Email address / username             | `user@gmail.com`      |
| **IMAP Password**       | Password or App Password             | `xxxx xxxx xxxx xxxx` |
| **Inbox Folder**        | Incoming mail folder                 | `INBOX`               |
| **Sent Folder**         | Outgoing mail folder                 | `[Gmail]/Sent Mail`   |
| **Poll Interval (min)** | How often to check for new emails    | `5` (1-1440)          |
| **Memory Source Tag**   | Metadata tag used in memory          | `email_monitor`       |
| **Max Body Length**     | Maximum characters of the email body | `4000` (100-50000)    |
| **Ingest Attachments**  | Store the attachments too            | `false`               |
| **Max Attachment Size (MB)** | Larger attachments are skipped  | `20` (1-100)          |
| **Max Attachments per Email** | Only the first N are stored    | `20` (0-100)          |
| **Excluded Attachment Types** | MIME types never stored, comma separated | `image/png, text/csv` |
| **Check Time Budget (minutes)** | No new email is started after it | `10` (1-50)      |

Turning **Ingest Attachments** on applies to the emails not stored yet: the attachments of the emails already
stored are not read again. The **Excluded Attachment Types** are matched against both the type the sender declared
and the type recognised from the bytes (text files such as CSV or XML are recognised as `text/plain`).

Folder names are sent quoted and encoded as IMAP requires (e.g. `[Gmail]/Sent Mail`, `Posta inviata`, non-ASCII
names). Changing the **Memory Source Tag** changes the names of the files of the new emails only; the emails stored
before keep theirs, and are still found by the cleanups (they filter by folder, UIDVALIDITY and UID).

---

## Gmail Notes

For Gmail you must use an **App Password** (not your account password):

1. Go to <https://myaccount.google.com/security>
2. Enable **2-Step Verification** if not already active
3. Search for *App passwords* and generate one for "Mail"
4. Use that password in the **IMAP Password** field

Gmail's Sent folder is named `[Gmail]/Sent Mail`.

---

## Plugin Structure

```
ccat_email_monitoring/
├── email_monitor.py        # Core logic (hooks, monitoring, IMAP handling, body extraction)
├── settings.py             # Pydantic settings model
├── crypt.py                # Encryption at rest of the secret settings
├── tests/                  # Test suite (run from the core root)
├── plugin.json             # Plugin manifest
└── README.md               # This guide
```

---

## Memory Document Format

Each email is stored as a text document in the following format:

```
[EMAIL - RECEIVED]
Date: 2025-01-15T10:30:00+00:00
From: sender@example.com
To: me@example.com
Cc:
Subject: Meeting tomorrow

Hi,
I'm writing to confirm the meeting scheduled for...
```

The body is made of the `text/plain` and `text/html` parts of the email that are not attachments, in order, the HTML
converted to plain text as a browser shows it: scripts and styles dropped, entities decoded, paragraphs and line
breaks kept (a `<div>` is a new line, `<br><br>` a blank line), the address of a link after its text, images
replaced by their alternative text. Of each `multipart/alternative` (the same content in more formats) only one
choice is used: the last one made of `text/plain` parts only, else the last one with some text. Attachments, and
everything inside them (e.g. an attached email), are ignored; an inline forwarded email is part of the body, the
copy of the bounced email in a delivery report is not. An email without text in a `text/plain` or `text/html` part
is stored with the body `(empty body)`.
Charsets are read as browsers and mail clients read them (WHATWG Encoding standard): a missing or unknown charset,
and an ASCII or latin-1 one, is read as UTF-8 when the text is valid UTF-8, else as windows-1252; the same goes for
the headers (RFC 2047 encoded words, raw 8-bit bytes). The body is truncated at **Max Body Length** characters.

The body is stored as the file `<tag>_<folder>_<uidvalidity>_<uid>.txt` (`<tag>` is the **Memory Source Tag** made
of letters, digits, `_` and `-`).

### Attachments

With **Ingest Attachments** on, the attachments of an email are the parts with an `attachment` disposition, and the
inline parts with a file name that are not text of the body nor an inline image of it (an image with a Content-ID:
logos, signatures). Each one is stored as the file `<tag>_<folder>_<uidvalidity>_<uid>_<n>_<name>.<ext>` and read by
the parser of its type, recognised from its bytes as for an upload. An attached email is stored as a text file with
its headers and its body (not its own attachments). The chunks of an attachment start with
`[ALLEGATO di '<subject>' da <from>, <date>] <file name>`, so that they say which email they come from.

An attachment is skipped, with the reason listed in the metadata of the body, when attachments are off
(`disabled`), it is empty (`empty`), larger than **Max Attachment Size** (`too_large`), of an **Excluded Attachment
Type** (`excluded_type`), of a type no parser of the Cat reads (`unsupported_type`), or beyond **Max Attachments per
Email** (`too_many`).

### Metadata

Every file of an email (its body and its attachments) carries:

- `source`: the name of the file
- `when`: the time of the ingestion (set by the core)
- `email_source_tag`: the **Memory Source Tag** (default `email_monitor`)
- `email_uid`: IMAP UID; `email_uidvalidity`: UIDVALIDITY of its folder
- `email_folder`: `inbox` or `sent`
- `email_direction`: `RECEIVED` or `SENT`
- `email_subject`, `email_from`, `email_to`, `email_date`, `email_message_id`

The body adds `email_attachments_ingested` (file names), `email_attachments_skipped` (`{name, reason}`) and
`email_attachments_failed` (file names given up after 3 attempts); an attachment adds `email_attachment_filename`,
`email_attachment_index` (its position among the attachments, from 1) and `email_attachment_mime` (its type;
`message/rfc822` for an attached email).

---

## Removing Emails from Memory

To delete all stored emails, delete their points by the configured **Memory Source Tag**, and their files (the
name of each file is the `source` of its points):

```python
import os

metadata = {"email_source_tag": "email_monitor"}
points, _ = await cat.vector_memory_handler.get_all_tenant_points("declarative", metadata=metadata, with_vectors=False)
await cat.vector_memory_handler.delete_tenant_points("declarative", metadata=metadata)
for source in {p.payload["metadata"]["source"] for p in points}:
    cat.file_manager.remove_file(os.path.join(cat.agent_key, source))
```

The UIDs of the deleted emails stay recorded as processed: they are not ingested again.

Avoid changing the embedder while a check is running: the re-embed of the core takes no lock, and an email stored
between its listing of the stored files and its deletion of the points would stay without points.

---

## How Scheduling Works

The plugin uses the **White Rabbit** core plugin to schedule periodic mailbox checks:

- When the plugin is activated, a scheduled job is created
- When settings are updated, the job is recreated with new parameters
- When the plugin is deactivated, the scheduled job is removed
- The job runs every N minutes (configurable via `poll_interval_minutes`)

---

## Security

- The IMAP password is stored in the plugin's settings (managed by the Cat), encrypted at rest. Using a dedicated **App Password** is strongly recommended.
- The plugin connects in **read-only mode** (`readonly=True`) to all IMAP folders: it never modifies, moves, or deletes any email.
- The state of the folders (UIDVALIDITY, UIDs done, failed attempts) is stored in the database using the Cat's CRUD
  settings system.

---

## Compatibility

Tested with **matteocacciola/grinning-cat-core** (production-ready fork).

---

## Author

**Matteo Cacciola**  
GitHub: [matteocacciola](https://github.com/matteocacciola)  
Plugin Repository: [ccat_email_monitoring](https://github.com/matteocacciola/ccat_email_monitoring)

---

## License

This plugin is provided as-is for use with the Grinning Cat framework.
