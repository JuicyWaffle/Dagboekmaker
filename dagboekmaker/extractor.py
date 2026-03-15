"""
dagboekmaker.extractor
~~~~~~~~~~~~~~~~~~~~~~
Detecteert en extraheert platte tekst uit bronbestanden in allerlei formaten.
Ondersteunde formaten: TXT, RTF, DOC/DOCX, WP (WordPerfect), ODT, PDF,
                        HTML, JPEG/PNG/TIFF (EXIF), EML/MSG.
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
    # plaintext
    ".txt": "plaintext",
    ".md": "plaintext",
    ".csv": "plaintext",
    # rich text / word processors
    ".rtf": "libreoffice",
    ".doc": "libreoffice",
    ".docx": "libreoffice",
    ".odt": "libreoffice",
    ".wps": "libreoffice",
    ".wpd": "libwpd",       # WordPerfect
    ".wp":  "libwpd",
    ".wp4": "libwpd",
    ".wp5": "libwpd",
    ".wp6": "libwpd",
    # PDF
    ".pdf": "pdfminer",
    # web
    ".html": "html",
    ".htm":  "html",
    # e-mail
    ".eml": "email",
    ".msg": "email",
    # afbeeldingen (EXIF-metadata + optioneel OCR)
    ".jpg":  "image",
    ".jpeg": "image",
    ".png":  "image",
    ".tif":  "image",
    ".tiff": "image",
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
        elif methode == "image":
            exif = _exif(p)
            tekst = None  # OCR optioneel via Tesseract
            return ExtractieResultaat(
                pad=str(pad), formaat=ext, methode=methode,
                tekst=tekst, exif=exif, bestandsdatum=bestandsdatum
            )
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


def _exif(p: Path) -> dict:
    """Leest EXIF-metadata via exiftool (vereist installatie)."""
    try:
        import json as _json
        result = subprocess.run(
            ["exiftool", "-json", "-DateTimeOriginal", "-CreateDate",
             "-GPSLatitude", "-GPSLongitude", "-Model", str(p)],
            capture_output=True, timeout=15
        )
        data = _json.loads(result.stdout.decode())
        return data[0] if data else {}
    except Exception:
        return {}
