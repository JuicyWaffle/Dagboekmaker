"""
dagboekmaker.extractor
~~~~~~~~~~~~~~~~~~~~~~
Detecteert en extraheert platte tekst uit bronbestanden in allerlei formaten.
Ondersteunde formaten: TXT, RTF, DOC/DOCX, WP (WordPerfect), ODT, PDF,
                        HTML, EML/MSG.
Video- en fotobestanden worden genegeerd.
"""

import os
import re
import subprocess
import tempfile
import logging
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)

EXTENSIONS = {
    # ── Platte tekst ──────────────────────────────────────────────────────
    ".txt":  "plaintext",
    ".text": "plaintext",
    ".md":   "plaintext",
    ".csv":  "plaintext",
    ".tsv":  "plaintext",
    ".log":  "plaintext",
    ".asc":  "plaintext",
    ".nfo":  "plaintext",
    ".ini":  "plaintext",
    ".cfg":  "plaintext",
    ".tex":  "plaintext",     # LaTeX bronbestanden
    ".latex":"plaintext",
    ".bib":  "plaintext",     # BibTeX
    ".xml":  "plaintext",
    ".json": "plaintext",
    ".yaml": "plaintext",
    ".yml":  "plaintext",

    # ── Rich text / Tekstverwerkers ───────────────────────────────────────
    ".rtf":  "libreoffice",   # Rich Text Format
    ".doc":  "libreoffice",   # Word 97-2003
    ".docx": "libreoffice",   # Word 2007+
    ".dot":  "libreoffice",   # Word template
    ".dotx": "libreoffice",
    ".docm": "libreoffice",   # Word macro-enabled
    ".odt":  "libreoffice",   # OpenDocument Text
    ".ott":  "libreoffice",   # OpenDocument Template
    ".sxw":  "libreoffice",   # StarWriter / OpenOffice 1.x
    ".stw":  "libreoffice",
    ".sdw":  "libreoffice",   # StarWriter 5.x
    ".wps":  "libreoffice",   # Microsoft Works
    ".wpt":  "libreoffice",   # Works template
    ".wri":  "libreoffice",   # Windows Write
    ".abw":  "libreoffice",   # AbiWord
    ".zabw": "libreoffice",   # AbiWord compressed
    ".pages":"libreoffice",   # Apple Pages (via LibreOffice 7+)
    ".hwp":  "libreoffice",   # Hangul Word Processor (Koreaans, maar soms in archieven)
    ".602":  "libreoffice",   # T602 (Oost-Europees)
    ".pdb":  "libreoffice",   # Palm Doc / Aportis

    # ── WordPerfect ───────────────────────────────────────────────────────
    ".wpd":  "libwpd",
    ".wp":   "libwpd",
    ".wp4":  "libwpd",
    ".wp5":  "libwpd",
    ".wp6":  "libwpd",
    ".wp7":  "libwpd",

    # ── Spreadsheets (vaak bevatten ze tekst/notities) ────────────────────
    ".xls":  "libreoffice",   # Excel 97-2003
    ".xlsx": "libreoffice",   # Excel 2007+
    ".xlsm": "libreoffice",
    ".xlsb": "libreoffice",
    ".ods":  "libreoffice",   # OpenDocument Spreadsheet
    ".sxc":  "libreoffice",   # StarCalc
    ".sdc":  "libreoffice",
    ".numbers":"libreoffice", # Apple Numbers
    ".wk1":  "libreoffice",   # Lotus 1-2-3
    ".wk3":  "libreoffice",
    ".wk4":  "libreoffice",
    ".wks":  "libreoffice",   # Microsoft Works Spreadsheet
    ".qpw":  "libreoffice",   # Quattro Pro

    # ── Presentaties (bevatten soms notities/tekst) ───────────────────────
    ".ppt":  "libreoffice",   # PowerPoint 97-2003
    ".pptx": "libreoffice",   # PowerPoint 2007+
    ".pps":  "libreoffice",
    ".ppsx": "libreoffice",
    ".odp":  "libreoffice",   # OpenDocument Presentation
    ".sxi":  "libreoffice",   # StarImpress
    ".key":  "libreoffice",   # Apple Keynote

    # ── PDF ───────────────────────────────────────────────────────────────
    ".pdf":  "pdfminer",

    # ── E-books ───────────────────────────────────────────────────────────
    ".epub": "epub",
    ".mobi": "mobi",
    ".fb2":  "plaintext",     # FictionBook XML
    ".djvu": "djvu",
    ".djv":  "djvu",

    # ── PostScript / XPS ──────────────────────────────────────────────────
    ".ps":   "ps",
    ".eps":  "ps",
    ".xps":  "xps",
    ".oxps": "xps",

    # ── Web / HTML ────────────────────────────────────────────────────────
    ".html": "html",
    ".htm":  "html",
    ".xhtml":"html",
    ".mht":  "html",          # MIME HTML (IE saved pages)
    ".mhtml":"html",
    ".shtml":"html",

    # ── E-mail ────────────────────────────────────────────────────────────
    ".eml":  "email",
    ".msg":  "msg",           # Outlook MSG (apart behandeld)
    ".mbox": "mbox",
    ".emlx": "email",         # Apple Mail

    # ── Chat / messaging ──────────────────────────────────────────────────
    ".vcf":  "plaintext",     # vCard
    ".ics":  "plaintext",     # iCalendar

    # ── Gecomprimeerde helppagina's ───────────────────────────────────────
    ".chm":  "chm",           # Compiled HTML Help
}


@dataclass
class ExtractieResultaat:
    pad: str
    formaat: str
    methode: str
    tekst: Optional[str] = None
    fout: Optional[str] = None
    exif: dict = field(default_factory=dict)
    bestandsdatum: Optional[str] = None


def extraheer(pad: str) -> ExtractieResultaat:
    """
    Hoofd-entry-point. Geeft een ExtractieResultaat terug ongeacht succes/falen.
    """
    p = Path(pad)
    ext = p.suffix.lower()
    methode = EXTENSIONS.get(ext, "onbekend")
    bestandsdatum = _bestandsdatum(p)

    try:
        if methode == "plaintext":
            tekst = _lees_plaintext(p)
        elif methode == "libreoffice":
            tekst = _via_libreoffice(p)
        elif methode == "libwpd":
            tekst = _via_libwpd(p)
        elif methode == "pdfminer":
            tekst = _via_pdfminer(p)
        elif methode == "html":
            tekst = _via_html(p)
        elif methode == "email":
            tekst = _via_email(p)
        elif methode == "msg":
            tekst = _via_msg(p)
        elif methode == "mbox":
            tekst = _via_mbox(p)
        elif methode == "epub":
            tekst = _via_epub(p)
        elif methode == "mobi":
            tekst = _via_mobi(p)
        elif methode == "djvu":
            tekst = _via_djvu(p)
        elif methode == "ps":
            tekst = _via_ps(p)
        elif methode == "xps":
            tekst = _via_libreoffice(p)  # LibreOffice kan XPS
        elif methode == "chm":
            tekst = _via_chm(p)
        else:
            return ExtractieResultaat(
                pad=str(pad), formaat=ext, methode="onbekend",
                fout=f"Geen extractor voor extensie '{ext}'",
                bestandsdatum=bestandsdatum
            )
    except Exception as e:
        log.warning("Extractie mislukt voor %s: %s", pad, e)
        return ExtractieResultaat(
            pad=str(pad), formaat=ext, methode=methode,
            fout=str(e), bestandsdatum=bestandsdatum
        )

    return ExtractieResultaat(
        pad=str(pad), formaat=ext, methode=methode,
        tekst=tekst, bestandsdatum=bestandsdatum
    )


# ── interne hulpfuncties ──────────────────────────────────────────────────────

def _bestandsdatum(p: Path) -> Optional[str]:
    try:
        ts = p.stat().st_mtime
        from datetime import datetime, timezone
        return datetime.fromtimestamp(ts, tz=timezone.utc).date().isoformat()
    except Exception:
        return None


def _lees_plaintext(p: Path) -> str:
    for enc in ("utf-8", "latin-1", "cp1252", "cp850"):
        try:
            return p.read_text(encoding=enc)
        except UnicodeDecodeError:
            continue
    return p.read_bytes().decode("utf-8", errors="replace")


def _via_libreoffice(p: Path) -> str:
    """Converteert via LibreOffice headless naar tijdelijk TXT-bestand."""
    with tempfile.TemporaryDirectory() as tmp:
        subprocess.run(
            ["libreoffice", "--headless", "--convert-to", "txt:Text",
             "--outdir", tmp, str(p)],
            check=True, capture_output=True, timeout=60
        )
        uit = Path(tmp) / (p.stem + ".txt")
        if uit.exists():
            return _lees_plaintext(uit)
        raise RuntimeError("LibreOffice produceerde geen uitvoer.")


def _via_libwpd(p: Path) -> str:
    """WordPerfect via wpd2text (onderdeel van libwpd-tools)."""
    result = subprocess.run(
        ["wpd2text", str(p)],
        capture_output=True, timeout=30
    )
    if result.returncode == 0:
        return result.stdout.decode("utf-8", errors="replace")
    # Fallback: probeer via LibreOffice
    return _via_libreoffice(p)


def _via_pdfminer(p: Path) -> str:
    try:
        from pdfminer.high_level import extract_text
        return extract_text(str(p)) or ""
    except ImportError:
        # Fallback: pdftotext (poppler)
        result = subprocess.run(
            ["pdftotext", str(p), "-"],
            capture_output=True, timeout=60
        )
        return result.stdout.decode("utf-8", errors="replace")


def _via_html(p: Path) -> str:
    try:
        from html.parser import HTMLParser

        class _Stripper(HTMLParser):
            def __init__(self):
                super().__init__()
                self.delen = []
                self._skip = False

            def handle_starttag(self, tag, attrs):
                if tag in ("script", "style"):
                    self._skip = True

            def handle_endtag(self, tag):
                if tag in ("script", "style"):
                    self._skip = False

            def handle_data(self, data):
                if not self._skip:
                    self.delen.append(data)

        s = _Stripper()
        s.feed(_lees_plaintext(p))
        return re.sub(r"\s{3,}", "\n\n", "".join(s.delen)).strip()
    except Exception as e:
        raise RuntimeError(f"HTML-parsing mislukt: {e}")


def _via_email(p: Path) -> str:
    import email as emaillib
    raw = p.read_bytes()
    msg = emaillib.message_from_bytes(raw)
    delen = []
    if msg.is_multipart():
        for part in msg.walk():
            ct = part.get_content_type()
            if ct == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    delen.append(payload.decode("utf-8", errors="replace"))
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            delen.append(payload.decode("utf-8", errors="replace"))
    return "\n".join(delen)


def _via_msg(p: Path) -> str:
    """Outlook .msg via python-ole of extract-msg, fallback naar strings."""
    try:
        import extract_msg
        msg = extract_msg.Message(str(p))
        delen = []
        if msg.subject:
            delen.append(f"Onderwerp: {msg.subject}")
        if msg.sender:
            delen.append(f"Van: {msg.sender}")
        if msg.date:
            delen.append(f"Datum: {msg.date}")
        if msg.body:
            delen.append(msg.body)
        msg.close()
        return "\n".join(delen)
    except ImportError:
        # Fallback: probeer ruwe tekst te extraheren
        return _lees_plaintext(p)


def _via_mbox(p: Path) -> str:
    """Verwerkt mbox-bestanden (meerdere e-mails in één bestand)."""
    import mailbox
    delen = []
    try:
        mbox = mailbox.mbox(str(p))
        for i, msg in enumerate(mbox):
            if i >= 200:  # max 200 berichten per mbox
                delen.append(f"\n... ({len(mbox) - 200} berichten overgeslagen)")
                break
            headers = []
            if msg["from"]:
                headers.append(f"Van: {msg['from']}")
            if msg["to"]:
                headers.append(f"Aan: {msg['to']}")
            if msg["subject"]:
                headers.append(f"Onderwerp: {msg['subject']}")
            if msg["date"]:
                headers.append(f"Datum: {msg['date']}")
            body = ""
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        payload = part.get_payload(decode=True)
                        if payload:
                            body = payload.decode("utf-8", errors="replace")
                            break
            else:
                payload = msg.get_payload(decode=True)
                if payload:
                    body = payload.decode("utf-8", errors="replace")
            delen.append("\n".join(headers) + "\n" + body)
        mbox.close()
    except Exception as e:
        delen.append(f"Mbox-fout: {e}")
    return "\n---\n".join(delen)


def _via_epub(p: Path) -> str:
    """EPUB via zipfile + HTML-stripping."""
    import zipfile
    teksten = []
    try:
        with zipfile.ZipFile(str(p)) as z:
            for name in z.namelist():
                if name.endswith((".html", ".xhtml", ".htm")):
                    raw = z.read(name).decode("utf-8", errors="replace")
                    teksten.append(_strip_html(raw))
    except Exception as e:
        raise RuntimeError(f"EPUB-extractie mislukt: {e}")
    return "\n\n".join(teksten)


def _via_mobi(p: Path) -> str:
    """MOBI/PRC via fallback naar ruwe tekstextractie."""
    # Geen standaard Python library; probeer ruwe tekst
    raw = p.read_bytes()
    tekst = raw.decode("utf-8", errors="replace")
    # Filter niet-printbare tekens
    tekst = re.sub(r"[^\x20-\x7E\n\r\t\xA0-\xFF]", "", tekst)
    tekst = re.sub(r"\s{5,}", "\n\n", tekst)
    return tekst.strip()[:100_000]


def _via_djvu(p: Path) -> str:
    """DjVu via djvutxt (onderdeel van djvulibre)."""
    result = subprocess.run(
        ["djvutxt", str(p)],
        capture_output=True, timeout=60
    )
    if result.returncode == 0:
        return result.stdout.decode("utf-8", errors="replace")
    raise RuntimeError(f"djvutxt mislukt: {result.stderr.decode()}")


def _via_ps(p: Path) -> str:
    """PostScript/EPS naar tekst via ps2txt of Ghostscript."""
    # Probeer ps2txt eerst
    try:
        result = subprocess.run(
            ["ps2txt", str(p)],
            capture_output=True, timeout=60
        )
        if result.returncode == 0:
            return result.stdout.decode("utf-8", errors="replace")
    except FileNotFoundError:
        pass
    # Fallback: ghostscript naar tekst
    result = subprocess.run(
        ["gs", "-sDEVICE=txtwrite", "-o", "-", str(p)],
        capture_output=True, timeout=60
    )
    if result.returncode == 0:
        return result.stdout.decode("utf-8", errors="replace")
    raise RuntimeError(f"PS-extractie mislukt: {result.stderr.decode()}")


def _via_chm(p: Path) -> str:
    """CHM (Compiled HTML Help) via extract_chmLib of 7z."""
    try:
        result = subprocess.run(
            ["7z", "e", str(p), "-so", "*.htm", "*.html"],
            capture_output=True, timeout=60
        )
        if result.returncode == 0:
            return _strip_html(result.stdout.decode("utf-8", errors="replace"))
    except FileNotFoundError:
        pass
    raise RuntimeError("CHM-extractie: 7z niet gevonden")


def _strip_html(raw: str) -> str:
    """Hulpfunctie: strip HTML tags."""
    from html.parser import HTMLParser

    class _S(HTMLParser):
        def __init__(self):
            super().__init__()
            self.delen = []
            self._skip = False
        def handle_starttag(self, t, a):
            if t in ("script", "style"): self._skip = True
        def handle_endtag(self, t):
            if t in ("script", "style"): self._skip = False
        def handle_data(self, d):
            if not self._skip: self.delen.append(d)

    s = _S()
    s.feed(raw)
    return re.sub(r"\s{3,}", "\n\n", "".join(s.delen)).strip()
