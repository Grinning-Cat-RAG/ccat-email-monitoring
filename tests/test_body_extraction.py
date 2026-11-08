"""The body stored in memory: the text/plain and text/html parts that are not attachments, the HTML converted to plain
text, one choice per multipart/alternative; attachments (and whatever they contain) never end up in the body.

Run from the core root, with the secrets the core requires (any value for the tests):
``CAT_CRYPTO_KEY=test CAT_CRYPTO_SALT=test python -m pytest cat/plugins/ccat-email-monitoring/tests``.

The Cat imports every ``.py`` file of the plugin, tests included: at import time this module needs only the stdlib,
the Cat, the plugin and hypothesis are loaded in ``setUpClass`` and in the tests.
"""
import base64
import email
import html
import re
import sys
import unittest
from email.message import EmailMessage
from pathlib import Path

PLUGIN_DIR = Path(__file__).resolve().parents[1]

MODULE = None


def _load():
    """Loads the plugin once, as the Cat loads it: whatever the name of its folder."""
    global MODULE
    if MODULE is not None:
        return
    from cat.looking_glass.mad_hatter.plugin import Plugin

    plugin = Plugin(str(PLUGIN_DIR))
    plugin._load_decorated_functions()
    hook_fn = next(h.function for h in plugin.hooks if h.name == "after_plugin_toggling_on_agent")
    MODULE = sys.modules[hook_fn.__module__]


def _parse(raw: str | bytes):
    raw = raw.encode("utf-8") if isinstance(raw, str) else raw
    return email.message_from_bytes(raw)


def _settings(**overrides):
    from unittest.mock import MagicMock

    values = {"max_body_length": 4000, "memory_source_tag": "email_monitor"}
    values.update(overrides)
    return MagicMock(**values)


HTML_ONLY = """\
MIME-Version: 1.0
Subject: html only
Content-Type: text/html; charset="utf-8"

<html><head><title>Not the body</title><style>p { color: red; }</style></head>
<body><p>Ciao&nbsp;Mario,</p><p>il preventivo &egrave; pronto &amp; firmato.</p>
<script>alert("no")</script><div>Riga uno<br>Riga   due</div>
<ul><li>primo</li><li>secondo</li></ul></body></html>
"""

MIXED_ALTERNATIVE_HTML_ONLY = """\
MIME-Version: 1.0
Subject: nested
Content-Type: multipart/mixed; boundary="OUT"

--OUT
Content-Type: multipart/alternative; boundary="IN"

--IN
Content-Type: text/html; charset="iso-8859-1"
Content-Transfer-Encoding: quoted-printable

<p>Perch=E9 no?</p>
--IN--
--OUT
Content-Type: text/plain; charset="utf-8"
Content-Disposition: attachment; filename="note.txt"

SEGRETO-ALLEGATO
--OUT--
"""


class TestExtractTextBody(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _load()

    def body(self, raw):
        return MODULE._extract_text_body(_parse(raw))

    def test_a_single_part_html_email_is_converted_to_text(self):
        """Regression: a non-multipart text/html email was stored as raw HTML."""
        self.assertEqual(
            self.body(HTML_ONLY),
            "Ciao Mario,\n\nil preventivo è pronto & firmato.\n\nRiga uno\nRiga due\nprimo\nsecondo",
        )

    def test_an_html_only_alternative_nested_in_a_mixed_message_is_used(self):
        """Regression: with no text/plain part the body was empty, stored as "(empty body)"."""
        self.assertEqual(self.body(MIXED_ALTERNATIVE_HTML_ONLY), "Perché no?")

    def test_the_plain_part_wins_over_the_html_alternative(self):
        msg = EmailMessage()
        msg.set_content("testo semplice")
        msg.add_alternative("<p>testo html</p>", subtype="html")
        self.assertEqual(MODULE._extract_text_body(_parse(msg.as_bytes())), "testo semplice")

    def test_a_blank_plain_part_falls_back_to_the_html_one(self):
        msg = EmailMessage()
        msg.set_content(" \n\t\n")
        msg.add_alternative("<p>solo html</p>", subtype="html")
        self.assertEqual(MODULE._extract_text_body(_parse(msg.as_bytes())), "solo html")

    def test_an_inline_html_part_next_to_a_plain_one_is_kept(self):
        """Regression: any text/plain part with text dropped every text/html part, alternative or not."""
        raw = """\
Content-Type: multipart/mixed; boundary="B"

--B
Content-Type: text/plain

vedi sotto
--B
Content-Type: text/html

<p>CONTENUTO-HTML</p>
--B--
"""
        self.assertEqual(self.body(raw), "vedi sotto\n\nCONTENUTO-HTML")

    def test_an_inline_forwarded_html_only_email_is_kept(self):
        """Regression: the HTML-only forwarded email was dropped because of the plain "see below" of the outer one."""
        inner = EmailMessage()
        inner.set_content("<p>INOLTRATO-HTML</p>", subtype="html")
        msg = EmailMessage()
        msg.set_content("vedi sotto")
        msg.add_attachment(inner, disposition="inline")
        self.assertEqual(MODULE._extract_text_body(_parse(msg.as_bytes())), "vedi sotto\n\nINOLTRATO-HTML")

    def test_each_alternative_is_chosen_on_its_own(self):
        """Regression: the plain part of one alternative dropped the HTML-only one of another."""
        first = EmailMessage()
        first.set_content("uno semplice")
        first.add_alternative("<p>uno html</p>", subtype="html")
        second = EmailMessage()
        second.set_content("<p>due html</p>", subtype="html")
        second.make_alternative()
        msg = EmailMessage()
        msg.make_mixed()
        msg.attach(first)
        msg.attach(second)
        raw = msg.as_bytes()
        self.assertEqual(raw.count(b"multipart/alternative"), 2)
        self.assertEqual(MODULE._extract_text_body(_parse(raw)), "uno semplice\n\ndue html")

    def test_the_plain_alternative_wins_over_a_related_html_one(self):
        """multipart/alternative of text/plain and multipart/related (HTML plus its images): the plain one."""
        msg = EmailMessage()
        msg.set_content("testo semplice")
        msg.add_alternative("<p>testo html</p>", subtype="html")
        msg.get_payload()[1].add_related(b"\x89PNG", maintype="image", subtype="png", cid="<logo>")
        raw = msg.as_bytes()
        self.assertIn(b"multipart/related", raw)
        self.assertEqual(MODULE._extract_text_body(_parse(raw)), "testo semplice")

    def test_alternatives_with_blank_attached_or_non_text_choices(self):
        cases = {
            # a blank plain choice falls back to the related HTML one
            "--A\nContent-Type: text/plain\n\n \n--A\nContent-Type: multipart/related; boundary=R\n\n"
            "--R\nContent-Type: text/html\n\n<p>html</p>\n--R--\n--A--\n": "html",
            # an attached choice is no choice
            "--A\nContent-Type: text/plain\nContent-Disposition: attachment\n\nallegato\n"
            "--A\nContent-Type: text/html\n\n<p>html</p>\n--A--\n": "html",
            # no text at all
            "--A\nContent-Type: text/calendar\n\nBEGIN:VCALENDAR\n--A\nContent-Type: image/png\n\nx\n--A--\n": "",
            # among the plain choices the last, the richest one for RFC 2046
            "--A\nContent-Type: text/plain\n\nprima\n--A\nContent-Type: text/plain\n\nseconda\n"
            "--A\nContent-Type: text/html\n\n<p>html</p>\n--A--\n": "seconda",
            # a choice mixing plain and HTML text is an HTML one: a plain-only choice wins over it
            "--A\nContent-Type: text/plain\n\nsolo testo\n--A\nContent-Type: multipart/mixed; boundary=M\n\n"
            "--M\nContent-Type: text/plain\n\nmisto\n--M\nContent-Type: text/html\n\n<p>misto html</p>\n--M--\n"
            "--A--\n": "solo testo",
            # among HTML choices the last one with text
            "--A\nContent-Type: text/html\n\n<p>prima</p>\n--A\nContent-Type: text/html\n\n<p>seconda</p>\n"
            "--A\nContent-Type: text/html\n\n<p> </p>\n--A--\n": "seconda",
            # nested alternatives
            "--A\nContent-Type: multipart/alternative; boundary=N\n\n--N\nContent-Type: text/html\n\n<p>h</p>\n"
            "--N\nContent-Type: text/plain\n\ninterno\n--N--\n--A\nContent-Type: text/html\n\n<p>esterno</p>\n"
            "--A--\n": "interno",
        }
        for body, expected in cases.items():
            with self.subTest(body=body):
                self.assertEqual(self.body(f'Content-Type: multipart/alternative; boundary="A"\n\n{body}'),
                                 expected)

    def test_a_multipart_without_boundary_has_no_body(self):
        """A multipart whose boundary is missing is not parsed into parts: no text, not a crash."""
        self.assertEqual(self.body("Content-Type: multipart/mixed\n\ntesto"), "")

    def test_a_single_part_non_text_email_has_no_body(self):
        """Regression: a non-multipart message was decoded whatever its content type (e.g. a PDF as text)."""
        raw = ("MIME-Version: 1.0\nContent-Type: application/pdf\nContent-Transfer-Encoding: base64\n\n"
               "JVBERi0xLjQKJcOkw7zDtsOfCg==\n")
        self.assertEqual(self.body(raw), "")

    def test_a_single_part_attachment_has_no_body(self):
        raw = "Content-Type: text/plain\nContent-Disposition: attachment; filename=a.txt\n\nallegato\n"
        self.assertEqual(self.body(raw), "")

    def test_the_disposition_is_parsed_not_searched_as_a_substring(self):
        """Regression: an inline part whose file name contains "attachment" was dropped, and an upper-case
        ``ATTACHMENT`` disposition was kept as body."""
        msg = EmailMessage()
        msg.set_content("corpo")
        msg.add_attachment("testo inline", disposition="inline", filename="attachment-notes.txt")
        msg.add_attachment("testo allegato", filename="b.txt")
        raw = msg.as_string().replace("Content-Disposition: attachment", "Content-Disposition: ATTACHMENT")
        self.assertIn("ATTACHMENT", raw)
        self.assertEqual(self.body(raw), "corpo\n\ntesto inline")

    def test_an_attached_email_does_not_leak_into_the_body(self):
        """Regression: the text of an email attached as ``message/rfc822`` was appended to the body."""
        inner = EmailMessage()
        inner["Subject"] = "inoltrata"
        inner.set_content("TESTO-INOLTRATO")
        msg = EmailMessage()
        msg.set_content("vedi allegato")
        msg.add_attachment(inner)
        self.assertEqual(MODULE._extract_text_body(_parse(msg.as_bytes())), "vedi allegato")

    def test_the_bounced_message_of_a_delivery_report_is_not_body(self):
        """A delivery status report holds a copy of the bounced email (already in memory): only its text is body."""
        raw = """\
Content-Type: multipart/report; report-type=delivery-status; boundary="R"

--R
Content-Type: text/plain

Consegna non riuscita.
--R
Content-Type: message/delivery-status

Reporting-MTA: dns; mx.example.com

Final-Recipient: rfc822; x@example.com
Action: failed
--R
Content-Type: message/rfc822

Subject: originale

TESTO-RIMBALZATO
--R--
"""
        self.assertEqual(self.body(raw), "Consegna non riuscita.")

    def test_an_inline_forwarded_email_is_part_of_the_body(self):
        inner = EmailMessage()
        inner.set_content("TESTO-INOLTRATO")
        msg = EmailMessage()
        msg.set_content("vedi sotto")
        msg.add_attachment(inner, disposition="inline")
        self.assertEqual(MODULE._extract_text_body(_parse(msg.as_bytes())), "vedi sotto\n\nTESTO-INOLTRATO")

    def test_unknown_or_non_text_charsets_fall_back_to_utf8(self):
        """Regression: an unknown charset raised LookupError, the email was never stored (nor marked as seen)."""
        for charset in ("x-unknown", "base64", "rot13", "idna", "undefined", "utf\\0", "é"):
            for subtype in ("plain", "html"):
                with self.subTest(charset=charset, subtype=subtype):
                    raw = (f'Content-Type: text/{subtype}; charset="{charset}"\n'
                           "Content-Transfer-Encoding: 8bit\n\n").encode() + "perché \xff".encode("utf-8")
                    self.assertEqual(self.body(raw), "perché \xff")

    def test_charsets_are_read_as_the_browsers_and_mail_clients_do(self):
        """Regression: labels Python does not know (or reads strictly) gave mojibake or U+FFFD: they are mapped as
        in the WHATWG Encoding standard; latin-1 and ASCII labels read valid UTF-8 as UTF-8, anything else as
        windows-1252."""
        cases = [
            ("iso-8859-1", "\u20ac \u201cx\u201d \xe9".encode("cp1252"), "\u20ac \u201cx\u201d \xe9"),
            ("us-ascii", "perch\xe9".encode("utf-8"), "perch\xe9"),
            ("us-ascii", "perch\xe9".encode("cp1252"), "perch\xe9"),
            ("iso-8859-1", "perch\xe9".encode("utf-8"), "perch\xe9"),
            ("ISO-8859-8-I", "\u05e9\u05dc\u05d5\u05dd".encode("iso-8859-8"), "\u05e9\u05dc\u05d5\u05dd"),
            ("windows-874", "\u0e2a\u0e27\u0e31\u0e2a\u0e14\u0e35".encode("cp874"), "\u0e2a\u0e27\u0e31\u0e2a\u0e14\u0e35"),
            ("x-mac-roman", "caf\xe9".encode("mac_roman"), "caf\xe9"),
            ("x-sjis", "\u65e5\u672c".encode("cp932"), "\u65e5\u672c"),
            ("gb2312", "\u954f".encode("gbk"), "\u954f"),  # a GBK-only character, labelled gb2312
            ("x-gbk", "\u4e2d\u6587".encode("gbk"), "\u4e2d\u6587"),
            ("gb_2312-80", "\u4e2d".encode("gbk"), "\u4e2d"),
            ("ks_c_5601-1987", "\ud55c\uad6d".encode("cp949"), "\ud55c\uad6d"),
            ("big5", "\u4e2d".encode("big5hkscs"), "\u4e2d"),
            ('"utf8"', "perch\xe9".encode("utf-8"), "perch\xe9"),
            (None, "perch\xe9".encode("cp1252"), "perch\xe9"),  # no label: UTF-8, else windows-1252
            ("x-unknown", "perch\xe9".encode("cp1252"), "perch\xe9"),
            ("punycode", b"abc-def", "abc-def"),  # Python codecs that are not charsets
            ("unicode_escape", b"a\\u0041", "a\\u0041"),
            ("utf-8", b"perch\xe9", "perch\ufffd"),  # a declared charset is honoured
        ]
        for charset, data, expected in cases:
            with self.subTest(charset=charset, data=data):
                self.assertEqual(MODULE._decode_bytes(data, charset), expected)

    def test_a_null_byte_in_the_charset_name_falls_back_to_utf8(self):
        """``str.decode`` raises ValueError (not LookupError) for a NUL in the codec name."""
        self.assertEqual(MODULE._decode_bytes("perché".encode(), "utf\x00"), "perché")

    def test_crlf_line_ends_become_lf_in_plain_parts_too(self):
        """Regression: IMAP returns CRLF messages, the text/plain body kept the CR (the HTML one did not)."""
        raw = (b'Content-Type: multipart/mixed; boundary="B"\r\n\r\n--B\r\nContent-Type: text/plain\r\n\r\n'
               b"A\r\nB\r\n--B\r\nContent-Type: text/plain\r\n\r\nC\rD\r\n--B--\r\n")
        self.assertEqual(self.body(raw), "A\nB\n\nC\nD")

    def test_a_missing_charset_is_utf8_and_a_declared_one_is_honoured(self):
        self.assertEqual(self.body("Content-Type: text/plain\n\nperché"), "perché")
        raw = b'Content-Type: text/html; charset="iso-8859-1"\n\n<p>perch\xe9</p>'
        self.assertEqual(self.body(raw), "perché")

    def test_empty_payloads_give_an_empty_body(self):
        for raw in ("Content-Type: text/plain\n\n", "Content-Type: text/html\n\n",
                    "Content-Type: text/html\n\n<html><head><title>x</title></head><body> </body></html>",
                    'Content-Type: multipart/mixed; boundary="B"\n\n--B\nContent-Type: text/plain\n\n\n--B--\n'):
            with self.subTest(raw=raw):
                self.assertEqual(self.body(raw), "")

    def test_malformed_html_does_not_break_the_extraction(self):
        cases = {
            "<p>aperto<div>mai chiuso": "aperto\nmai chiuso",
            "</p></div>orfano</b>": "orfano",
            "prima<script>var x = '</p>'; dopo": "prima",
            "<p>a &bogus; &#xZZ; &#99999999; b</p>": "a &bogus; &#xZZ; \ufffd b",
            "<<<>>> <a href='x>testo": "<<<>>>",
            "<style>p{}</style><scr<script>ipt>x</script>ok": "ipt>xok",  # "scr<script" is the tag
            "<!-- commento --><![CDATA[x]]><?php echo 1 ?>fine": "fine",
        }
        for markup, expected in cases.items():
            with self.subTest(markup=markup):
                self.assertEqual(self.body(f"Content-Type: text/html\n\n{markup}"), expected)

    def test_self_closing_tags(self):
        """``<br/>`` breaks the line; a self-closing ``<script/>`` hides nothing after it."""
        self.assertEqual(MODULE._html_to_text("a<br/>b<script/>visibile<p/>c"), "a\nbvisibile\n\nc")

    def test_a_closing_br_breaks_the_line_like_the_browsers_do(self):
        self.assertEqual(MODULE._html_to_text("a</br>b"), "a\nb")

    def test_an_unclosed_head_does_not_hide_the_body(self):
        """The browsers close ``<head>`` at ``<body>``: its title and styles are dropped, the body is kept."""
        markup = "<html><head><title>T</title><style>p{}</style><meta charset=utf-8><body><p>visibile"
        self.assertEqual(MODULE._html_to_text(markup), "visibile")

    def test_table_cells_are_separated_and_rows_are_lines(self):
        markup = "<table><tr><td>a</td><td>b</td><td> c</td></tr><tr><th>d</th></tr></table>e"
        self.assertEqual(MODULE._html_to_text(markup), "a b c\nd\n\ne")
        # an empty last cell leaves no trailing space before the break
        self.assertEqual(MODULE._html_to_text("<table><tr><td>a</td><td></td></tr></table>b"), "a\n\nb")

    def test_preformatted_text_keeps_its_lines(self):
        self.assertEqual(self.body("Content-Type: text/html\n\n<pre>a  b\n  c</pre><p>d</p>"), "a  b\n  c\n\nd")

    def test_preformatted_text_keeps_its_blank_lines_and_spaces(self):
        """Regression: three or more line ends, and the spaces at the end, were collapsed inside ``<pre>`` too."""
        self.assertEqual(MODULE._html_to_text("<pre>a\n\n\n\nb  </pre><p>c</p>"), "a\n\n\n\nb  \n\nc")
        self.assertEqual(MODULE._html_to_text("<pre>a\n\n</pre><p>b</p>"), "a\n\nb")

    def test_two_line_breaks_are_a_blank_line(self):
        """Regression: ``<br><br>`` (the paragraph of Gmail and Outlook) became a single line break."""
        self.assertEqual(MODULE._html_to_text("a<br><br>b"), "a\n\nb")
        self.assertEqual(MODULE._html_to_text("a<br><br><br><br>b"), "a\n\nb")
        self.assertEqual(MODULE._html_to_text("<p>a</p><br><br><p>b</p>"), "a\n\nb")
        self.assertEqual(MODULE._html_to_text("<ul><li>a</li><li>b</li></ul>"), "a\nb")

    def test_a_stray_closing_pre_does_not_keep_whitespace(self):
        self.assertEqual(MODULE._html_to_text("</pre>a    b\n\n\n c"), "a b c")

    def test_a_long_html_body_is_converted_in_linear_time(self):
        """Many breaks in a large body: the conversion does not copy the text at each break."""
        import time

        started = time.monotonic()
        text = MODULE._html_to_text("<pre>x</pre>riga<br>" * 100_000)
        self.assertLess(time.monotonic() - started, 5)
        self.assertEqual(text.count("riga"), 100_000)

    def test_the_newline_after_pre_and_its_leading_spaces(self):
        """Regression: the line end right after ``<pre>`` was kept (the browsers drop it), and the leading spaces of
        preformatted text at the start of the body were stripped."""
        self.assertEqual(MODULE._html_to_text("x<pre>\ncode</pre>y"), "x\n\ncode\n\ny")
        self.assertEqual(MODULE._html_to_text("<p>a</p><pre>\n</pre><p>b</p>"), "a\n\nb")
        self.assertEqual(MODULE._html_to_text("<pre>  code\n  more</pre>"), "  code\n  more")
        self.assertEqual(MODULE._html_to_text("<pre><b>x</b>\ny</pre>"), "x\ny")  # not right after <pre>
        self.assertEqual(self.body("Content-Type: text/html\n\n<pre>   </pre>"), "")

    def test_block_elements_break_the_text(self):
        """Regression: <center>, <option> and other block elements glued the words together."""
        cases = {
            "a<center>b</center>c": "a\nb\nc",
            "<select><option>1</option><option>2</option></select>": "1\n2",
            "<table><caption>t</caption><tr><td>x</td></tr></table>": "t\nx",
            "<details><summary>s</summary>d</details>": "s\nd",
            "<figure>f<figcaption>c</figcaption></figure>": "f\nc",
            "<fieldset><legend>l</legend>x</fieldset>": "l\nx",
            "<menu><li>m1</li><li>m2</li></menu>": "m1\nm2",
        }
        for markup, expected in cases.items():
            with self.subTest(markup=markup):
                self.assertEqual(MODULE._html_to_text(markup), expected)

    def test_divs_and_lists_are_lines_as_the_browsers_show_them(self):
        """Gmail and Apple Mail put each typed line in a <div>: a line break, not a paragraph; their blank line is
        <div><br></div>."""
        cases = {
            "<div>a</div><div>b</div>": "a\nb",
            "<div>a</div><div><br></div><div>b</div>": "a\n\nb",
            "<ol><li>a<ul><li>b</li></ul></li><li>c</li></ol>": "a\nb\nc",
            "<p>a</p><div>b</div>": "a\n\nb",
        }
        for markup, expected in cases.items():
            with self.subTest(markup=markup):
                self.assertEqual(MODULE._html_to_text(markup), expected)

    def test_links_keep_their_url_and_images_their_alt_text(self):
        cases = {
            '<a href="https://x.it/p?q=1&amp;r=2">vedi</a> qui': "vedi (https://x.it/p?q=1&r=2) qui",
            '<a href="https://x.it/">https://x.it</a>': "https://x.it",
            '<a href="mailto:a@b.it">a@b.it</a>': "a@b.it",
            '<a href="mailto:a@b.it">scrivimi</a>': "scrivimi (mailto:a@b.it)",
            '<a href="#top">su</a> <a href="javascript:x()">js</a> <a href="">vuoto</a> <a>nessuno</a>':
                "su js vuoto nessuno",
            '<a href="https://u.it"><img src="b.png" alt="Banner  estivo"></a>': "Banner estivo (https://u.it)",
            '<a href="https://u.it"><img src="b.png"></a>': "https://u.it",
            '<img src="l.png" alt="Logo"/> testo <img alt="">': "Logo testo",
            '<template><a href="https://n.it">n</a><img alt="n"></template>ok': "ok",
            '<a href="https://a.it">a<a href="https://b.it">b</a>': "ab (https://b.it)",  # unclosed outer link
            '</a>orfano': "orfano",
        }
        for markup, expected in cases.items():
            with self.subTest(markup=markup):
                self.assertEqual(MODULE._html_to_text(markup), expected)

    def test_templates_are_not_text(self):
        self.assertEqual(MODULE._html_to_text("a<template><p>nascosto</p></template>b"), "ab")


class TestDocumentHeaders(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        _load()

    def test_an_unknown_header_charset_does_not_drop_the_email(self):
        """Regression: an RFC 2047 word in an unknown charset raised LookupError and the email was dropped."""
        raw = ("Subject: =?x-unknown?Q?perch=C3=A9?=\nFrom: a@b.c\nTo: d@e.f\n"
               "Content-Type: text/html; charset=utf-8\n\n<p>ciao</p>")
        doc = MODULE._build_document(_parse(raw), "7", "inbox", _settings())
        self.assertIsNotNone(doc)
        self.assertEqual(doc.metadata["email_subject"], "perché")
        self.assertTrue(doc.page_content.endswith("\n\nciao"))

    def test_encoded_words_keep_the_header_spacing(self):
        """Regression: the decoded chunks were joined with a space: doubled spaces around an encoded word, and a
        space between two adjacent encoded words (RFC 2047 drops it)."""
        for raw, expected in (("Re: =?utf-8?q?ciao?= mondo", "Re: ciao mondo"),
                              ("=?utf-8?q?a?= =?iso-8859-1?q?b?=", "ab"),
                              ("plain subject", "plain subject"),
                              ("=?x-unknown?q?perch=C3=A9?= ok", "perché ok")):
            with self.subTest(raw=raw):
                self.assertEqual(MODULE._decode_header_value(raw), expected)

    def test_a_character_split_across_encoded_words(self):
        """Regression: adjacent encoded words were decoded one by one, a character split between them was lost."""
        for raw, expected in (("=?utf-8?b?4oI=?==?utf-8?b?rA==?=", "\u20ac"),  # split at a byte boundary
                              ("=?utf-8?q?=E2=82?= =?utf-8?q?=AC?=", "\u20ac"),
                              ("=?UTF-8?q?=E2=82?=\r\n =?utf-8?Q?=AC_ok?=", "\u20ac ok"),
                              ("=?utf-8?q?=C3?= x =?utf-8?q?=A9?=", "\ufffd x \ufffd"),  # not adjacent
                              ("=?utf-8?q?a?= =?iso-8859-1?q?=E9?=", "a\xe9")):
            with self.subTest(raw=raw):
                self.assertEqual(MODULE._decode_header_value(raw), expected)

    def test_raw_8bit_header_bytes_that_are_not_utf8(self):
        msg = _parse(b"Subject: caf\xe9\n\nbody")
        self.assertEqual(MODULE._decode_header_value(msg.get("Subject")), "caf\xe9")

    def test_a_malformed_encoded_word_does_not_drop_the_email(self):
        """Regression: a bad base64 encoded word raised HeaderParseError, the email was never stored (nor marked as
        seen) and was fetched again at every check."""
        for raw, expected in (("=?utf-8?b?a?=", "=?utf-8?b?a?="),
                              ("=?utf-8?B?QQ?= <a@b.c>, =?utf-8?b?x?= <d@e.f>", "A <a@b.c>, =?utf-8?b?x?= <d@e.f>"),
                              ("=?utf-8?q?ok?= =?utf-8?b?!!?=", "ok =?utf-8?b?!!?=")):
            with self.subTest(raw=raw):
                self.assertEqual(MODULE._decode_header_value(raw), expected)
        doc = MODULE._build_document(_parse("Subject: =?utf-8?b?a?=\n\nciao"), "9", "inbox", _settings())
        self.assertIsNotNone(doc)
        self.assertEqual(doc.metadata["email_subject"], "=?utf-8?b?a?=")

    def test_raw_8bit_text_next_to_an_encoded_word(self):
        """Regression: raw UTF-8 bytes in a header left the encoded word next to them undecoded, and non-ASCII text
        next to an encoded word became U+FFFD."""
        msg = _parse(b"Subject: perch\xc3\xa9 =?utf-8?q?x?=\n\nbody")
        self.assertEqual(MODULE._decode_header_value(msg.get("Subject")), "perch\xe9 x")
        self.assertEqual(MODULE._decode_header_value("=?utf-8?q?a?= perch\xe9"), "a perch\xe9")

    def test_folded_and_mixed_encoded_words(self):
        for raw, expected in (("=?utf-8?q?a?=\r\n =?utf-8?q?b?=", "ab"),  # folded: still adjacent words
                              ("=?utf-8?q?a_b?=", "a b"),  # Q: underscore is a space
                              ("=?UTF-8?Q?=C3=A9?=", "\xe9"),
                              ("=?utf-8*it?q?x?=", "x"),  # RFC 2231 language suffix
                              ("=?utf-8?b?w6k=?=", "\xe9"),
                              ("=?utf-8?b?w6k?=", "\xe9"),  # missing padding
                              ("a =?utf-8?q?b?= c", "a b c"),
                              ("", ""),
                              (None, "")):  # no header
            with self.subTest(raw=raw):
                self.assertEqual(MODULE._decode_header_value(raw), expected)

    def test_an_email_without_text_is_stored_as_empty_body(self):
        raw = "Subject: s\nContent-Type: image/png\nContent-Transfer-Encoding: base64\n\niVBORw0KGgo=\n"
        doc = MODULE._build_document(_parse(raw), "8", "sent", _settings())
        self.assertTrue(doc.page_content.endswith("\n\n(empty body)"))


class TestBodyProperties(unittest.TestCase):
    """Invariants, over generated MIME trees: the extraction never raises; nothing inside an attachment reaches the
    body; inside each multipart/alternative only one choice reaches the body, the last one made of text/plain parts
    only, else the last one with some text (RFC 2046); every other text part reaches it; HTML-escaped text
    round-trips to the same words."""

    @classmethod
    def setUpClass(cls):
        _load()

    def test_invariants_on_generated_messages(self):
        from hypothesis import HealthCheck, given, settings, strategies as st

        charsets = st.sampled_from([None, "utf-8", "iso-8859-1", "x-unknown", "base64", "idna", ""])
        dispositions = st.sampled_from([None, "inline", "attachment", "ATTACHMENT", 'inline; filename="attachment"'])
        words = st.text(alphabet=st.characters(codec="utf-8", exclude_categories=("Cs", "Cc")), max_size=30)
        leaf_types = st.sampled_from(["text/plain", "text/html", "application/pdf", "image/png"])

        counter = iter(range(10 ** 9))
        blanks = st.text(alphabet=" \t\n\xa0", max_size=5)
        lines = st.lists(words, min_size=1, max_size=3).map("\n".join)  # line ends: LF here, CRLF in the raw
        encodings = st.sampled_from(["base64", "8bit"])

        def leaf(draw_type, disposition, charset, text, encoding, blank):
            """A text leaf, or (blank) a leaf with only whitespace and no marker: no text for the body."""
            return ("leaf", draw_type, disposition, charset, text, encoding, None if blank else next(counter))

        leaves = st.one_of(st.builds(leaf, leaf_types, dispositions, charsets, lines, encodings, st.just(False)),
                           st.builds(leaf, st.sampled_from(["text/plain", "text/html"]), dispositions, charsets,
                                     blanks, encodings, st.just(True)))
        trees = st.recursive(
            leaves,
            lambda children: st.one_of(
                st.tuples(st.just("multi"), st.sampled_from(["mixed", "alternative", "related", "x-unknown"]),
                          dispositions, st.lists(children, min_size=1, max_size=4)),
                # alternatives with more choices, often blank, plain, HTML or mixed: the choice is exercised
                st.tuples(st.just("multi"), st.just("alternative"), st.sampled_from([None, "inline"]),
                          st.lists(children, min_size=2, max_size=4)),
                st.tuples(st.just("message"), dispositions, children),  # message/rfc822, inline or attached
            ),
            max_leaves=8,
        )

        def disposition_header(disposition):
            return f"\nContent-Disposition: {disposition}" if disposition else ""

        def attached_by(disposition):
            return (disposition or "").lower().startswith("attachment")

        def render(node):
            """MIME text of the node, plus its marked leaves as (type, inside an attachment, charset, text, marker)."""
            if node[0] == "leaf":
                _, ctype, disposition, charset, text, encoding, n = node
                marker = "" if n is None else f"MRK{n}X"
                body = f"{html.escape(text)} {marker}" if ctype == "text/html" else f"{text} {marker}"
                headers = f"Content-Type: {ctype}" + (f'; charset="{charset}"' if charset is not None else "")
                headers += f"\nContent-Transfer-Encoding: {encoding}" + disposition_header(disposition)
                # 8bit: the text in the raw message, its line ends become CRLF with the message's
                encoded = base64.b64encode(body.encode("utf-8")).decode() if encoding == "base64" else body
                found = [] if n is None else [(ctype, attached_by(disposition), charset, text, marker)]
                return f"{headers}\n\n{encoded}\n", found
            if node[0] == "message":
                _, disposition, child = node
                text, child_leaves = render(child)
                headers = "Content-Type: message/rfc822" + disposition_header(disposition)
                attached = attached_by(disposition)
                return (f"{headers}\n\nSubject: inoltrata\n{text}",
                        [(t, a or attached, c, s, m) for t, a, c, s, m in child_leaves])
            _, subtype, disposition, children = node
            boundary = f"B{next(counter)}"
            headers = f'Content-Type: multipart/{subtype}; boundary="{boundary}"' + disposition_header(disposition)
            out, found = [f"{headers}\n\n"], []
            attached = attached_by(disposition)
            for child in children:
                text, child_leaves = render(child)
                out.append(f"--{boundary}\n{text}")
                found += [(t, a or attached, c, s, m) for t, a, c, s, m in child_leaves]
            out.append(f"--{boundary}--\n")
            return "".join(out), found

        def expected(node):
            """Markers the body keeps, as (marker, from HTML) in order: the model of the rule, on the tree."""
            if node[0] == "message":
                return [] if attached_by(node[1]) else expected(node[2])
            if attached_by(node[2]):
                return []
            if node[0] == "leaf":
                _, ctype, _, _, _, _, n = node
                if n is None or ctype not in ("text/plain", "text/html"):
                    return []
                return [(f"MRK{n}X", ctype == "text/html")]
            children = [expected(child) for child in node[3]]
            if node[1] != "alternative":
                return [kept for child in children for kept in child]
            filled = [child for child in children if child]
            plain = [child for child in filled if not any(from_html for _, from_html in child)]
            return (plain or filled or [[]])[-1]

        @settings(max_examples=600, deadline=None, suppress_health_check=[HealthCheck.too_slow])
        @given(trees, st.booleans())
        def check(tree, crlf):
            raw, found = render(tree)
            if crlf:  # as IMAP returns them
                raw = raw.replace("\n", "\r\n")
            text = MODULE._extract_text_body(_parse(raw))
            self.assertIsInstance(text, str)
            self.assertNotIn("\r", text)
            kept = [marker for marker, _ in expected(tree)]
            # the kept markers, in document order
            self.assertEqual(kept, re.findall(r"MRK\d+X", text))
            for ctype, attached, charset, source, marker in found:
                if attached or marker not in kept:
                    continue
                if ctype == "text/html":
                    normalised = re.sub(r"\s+", " ", f"{' '.join(source.split())} {marker}".strip())
                    self.assertIn(normalised, re.sub(r"\s+", " ", text))
                else:
                    self.assertIn(f"{source} {marker}".strip(), text)

        check()

    def test_any_html_never_raises(self):
        from hypothesis import given, settings, strategies as st

        markup = st.lists(st.one_of(
            st.sampled_from(["<p>", "</p>", "<br>", "<script>", "</script>", "<style>", "</style>", "<pre>",
                             "</pre>", "<head>", "</head>", "<!--", "-->", "&amp;", "&#", "&", "<", ">", "<li",
                             "<br/>", "<SCRIPT>", "</SCRIPT >", "\n", " ", "\xa0", '<a href="https://u.it">',
                             "</a>", '<img alt="x">', "<div>", "</div>", "<td>", "<template>", "</template>"]),
            st.text(max_size=10)), max_size=40).map("".join)

        @settings(max_examples=400, deadline=None)
        @given(markup)
        def check(source):
            text = MODULE._html_to_text(source)
            self.assertIsInstance(text, str)
            if "<pre" not in source:  # preformatted text keeps its own spaces and blank lines
                self.assertEqual(text, text.strip())
                self.assertNotIn("\n\n\n", text)
            self.assertEqual(text, text.strip("\n"))

        check()


if __name__ == "__main__":
    unittest.main()
