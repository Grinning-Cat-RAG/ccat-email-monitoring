# 📬 Email Monitor Plugin — Grinning Cat AI

Monitors an email mailbox via IMAP and automatically stores every **incoming** (Inbox) and **outgoing** (Sent) email into the Cat's **declarative vector memory**.

---

## How It Works

1. The plugin connects to the IMAP server using the credentials configured in the plugin *Settings*.
2. It periodically scans (every N minutes, configurable) both the Inbox and Sent folders.
3. New emails (not yet processed) are formatted as text documents and ingested into the *declarative memory* through the **RabbitHole** pipeline.
4. The UIDs of already-processed emails are saved in the database to avoid duplicates between polling cycles.

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

Each document carries the following metadata:

- `source`: configurable tag (default `email_monitor`)
- `when`: Unix timestamp of the email date
- `email_uid`: unique IMAP UID
- `email_folder`: `inbox` or `sent`
- `email_direction`: `RECEIVED` or `SENT`
- `email_subject`, `email_from`, `email_to`, `email_date`

---

## Removing Emails from Memory

To delete all stored emails from the vector memory, delete the declarative points by their source tag
(use the configured **Memory Source Tag**):

```python
await cat.vector_memory_handler.delete_tenant_points("declarative", metadata={"source": "email_monitor"})
```

The UIDs of the deleted emails stay recorded as processed: they are not ingested again.

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
- Already-processed email UIDs are stored in the database using the Cat's CRUD settings system.

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
