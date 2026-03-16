"""
dagboekmaker.verrijking
~~~~~~~~~~~~~~~~~~~~~~~
Verrijkt een geëxtraheerd document met:
  - documenttype
  - samenvatting
  - thema's
  - emotionele toon
  - actoren (namen + aliassen + relatie)
  - levensperiode-indeling
  - narratieve spanning
  - scriptwriter-velden (locatie, dialoog, conflict, scène-potentieel)

Kan draaien met de Anthropic API (Claude) of een lokale Ollama-instantie.
Ondersteunt vision (afbeeldingen) via beide backends.
"""

import base64
import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# Maximale afbeeldingsgrootte voor de API (bytes)
_MAX_IMAGE_BYTES = 5 * 1024 * 1024  # 5 MB

_IMAGE_MIME = {
    ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".gif": "image/gif",
    ".webp": "image/webp",
}

DOCUMENT_TYPES = [
    # Persoonlijk
    "dagboek", "brief", "notitie", "foto", "ansichtkaart",
    # Communicatie
    "e-mail", "sms", "chat", "fax",
    # Zakelijk / werk
    "rapport", "memo", "notulen", "presentatie", "cv", "sollicitatie",
    # Financieel / juridisch
    "financieel", "factuur", "contract", "akte", "belasting", "verzekering",
    # Officieel
    "paspoort", "rijbewijs", "diploma", "certificaat", "attest",
    "medisch", "recept",
    # Creatief
    "gedicht", "verhaal", "essay", "manuscript", "scenario",
    # Media / sociaal
    "artikel", "knipsel", "blog", "forumpost",
    # Huishoudelijk
    "recept_koken", "boodschappenlijst", "to-do",
    # Technisch
    "code", "configuratie", "logbestand",
    # Overig
    "onbekend",
]

THEMAS = [
    "familie", "verlies", "liefde", "geld", "ambitie", "ziekte",
    "vriendschap", "verhuizing", "werk", "identiteit", "conflict",
    "vergeving", "vrijheid", "eenzaamheid", "vreugde", "onderwijs",
    "religie", "politiek", "natuur", "reizen",
    # Uitbreiding voor intieme archieven
    "seksualiteit", "intimiteit", "verlangen", "schuld", "geheimen",
    "relaties", "zelfreflectie",
]

TONEN = ["blij", "verdrietig", "bezorgd", "boos", "nostalgisch",
         "neutraal", "hoopvol", "angstig", "ironisch", "liefdevol"]

SYSTEM_PROMPT = """Je bent een literair-historische analist die persoonlijke archiefdocumenten
verwerkt voor een autobiografisch scriptproject. Je taak is een document te analyseren
en een JSON-object terug te geven met de volgende velden.

BELANGRIJK: Dit is een serieus literair-autobiografisch project. Alle inhoud, inclusief
seksuele, intieme of expliciet erotische passages, heeft literaire waarde en MOET volledig
worden geanalyseerd. Censureer of filter NIETS. Als een document seksuele of expliciet
erotische inhoud bevat, markeer dit met "18plus": true en vermeld dit als thema.

Als je een AFBEELDING ontvangt (foto van een handgeschreven dagboekpagina, brief, etc.),
lees dan eerst de tekst op de afbeelding en analyseer die. Als de afbeelding geen tekst
bevat (puur een foto), beschrijf dan wat je ziet en analyseer de context.

{
  "type": "<één van: dagboek, brief, notitie, foto, ansichtkaart, e-mail, sms, chat, fax, rapport, memo, notulen, presentatie, cv, sollicitatie, financieel, factuur, contract, akte, belasting, verzekering, paspoort, rijbewijs, diploma, certificaat, attest, medisch, recept, gedicht, verhaal, essay, manuscript, scenario, artikel, knipsel, blog, forumpost, recept_koken, boodschappenlijst, to-do, code, configuratie, logbestand, onbekend>",
  "samenvatting": "<maximaal 2 zinnen, feitelijk — wees eerlijk over de inhoud, ook als die intiem is>",
  "taal": "<ISO 639-1 taalcode, bv 'nl', 'fr', 'en'>",
  "themas": ["<kies uit: familie, verlies, liefde, geld, ambitie, ziekte, vriendschap, verhuizing, werk, identiteit, conflict, vergeving, vrijheid, eenzaamheid, vreugde, onderwijs, religie, politiek, natuur, reizen, seksualiteit, intimiteit, verlangen, schuld, geheimen, relaties, zelfreflectie>"],
  "emotionele_toon": "<één van: blij, verdrietig, bezorgd, boos, nostalgisch, neutraal, hoopvol, angstig, ironisch, liefdevol — kies 'neutraal' ALLEEN als er werkelijk geen emotie in het document zit>",
  "18plus": <true als het document seksuele, erotische of expliciet intieme inhoud bevat, anders false>,
  "actoren": [
    {
      "naam": "<naam zoals in document>",
      "rol": "<auteur|ontvanger|vermeld>",
      "geschatte_leeftijd": "<getal als afleidbaar uit context, anders null>",
      "relatie_tot_auteur": "<partner|ex|minnaar|vriend|familie|collega|kennis|onbekend>"
    }
  ],
  "narratief": {
    "spanning": <0.0–1.0, hoger = dramatisch belangrijker>,
    "keerpunt": <true|false>,
    "notitie": "<optionele redactionele noot voor scriptschrijver>"
  },
  "scriptwriter": {
    "locatie": "<waar speelt dit? stad, huis, café, straat — of 'onbekend'>",
    "tijdstip_dag": "<ochtend|middag|avond|nacht|onbekend>",
    "seizoen": "<lente|zomer|herfst|winter|onbekend>",
    "dialoog_fragmenten": ["<letterlijke citaten uit de tekst die als dialoog kunnen dienen, max 3>"],
    "zintuiglijke_details": ["<geuren, kleuren, geluiden, texturen die de schrijver noemt, max 3>"],
    "conflict": {
      "type": "<intern|extern|relationeel|moreel|geen>",
      "beschrijving": "<1 zin over het conflict, of leeg als geen conflict>",
      "tegenover": "<naam van tegenpartij, of 'zelf' bij intern conflict, of leeg>"
    },
    "scene_potentieel": <0.0–1.0, hoe goed vertaalbaar naar een filmscène>,
    "dramatische_functie": "<1 zin: wat doet deze entry voor het grotere verhaal?>"
  },
  "datering_hints": {
    "expliciete_vermeldingen": ["<citaat uit document dat datum impliceert>"],
    "cultuurverwijzingen": ["<technologie, nieuws, prijzen, etc.>"],
    "leeftijdsverwijzingen": ["<bv 'ik word volgende week 18'>"]
  }
}

Geef ALLEEN het JSON-object terug, zonder uitleg of markdown-codeblokken.
"""


@dataclass
class VerrijkingResultaat:
    type:             str = "onbekend"
    samenvatting:     str = ""
    taal:             str = "nl"
    themas:           list = field(default_factory=list)
    emotionele_toon:  str = "neutraal"
    actoren:          list = field(default_factory=list)
    narratief:        dict = field(default_factory=dict)
    scriptwriter:     dict = field(default_factory=dict)
    datering_hints:   dict = field(default_factory=dict)
    achttienplusinhoud: bool = False
    fout:             Optional[str] = None


def _lees_en_resize_afbeelding(pad: str) -> Optional[tuple[str, str]]:
    """Leest een afbeelding, resize indien nodig, geeft (base64_data, mime_type) terug."""
    p = Path(pad)
    if not p.exists():
        return None

    mime = _IMAGE_MIME.get(p.suffix.lower())
    if not mime:
        return None

    data = p.read_bytes()

    # Resize als te groot
    if len(data) > _MAX_IMAGE_BYTES:
        try:
            from PIL import Image
            import io
            img = Image.open(io.BytesIO(data))
            # Halveer afmetingen tot onder de limiet
            while len(data) > _MAX_IMAGE_BYTES:
                w, h = img.size
                img = img.resize((w // 2, h // 2), Image.LANCZOS)
                buf = io.BytesIO()
                fmt = "JPEG" if mime == "image/jpeg" else "PNG"
                img.save(buf, format=fmt, quality=85)
                data = buf.getvalue()
            mime = "image/jpeg" if fmt == "JPEG" else "image/png"
        except Exception as e:
            log.warning("Afbeelding resize mislukt voor %s: %s", pad, e)
            return None

    return base64.standard_b64encode(data).decode("ascii"), mime


class Verrijker:
    """
    Abstracte verrijker. Gebruik AnthropicVerrijker of OllamaVerrijker.
    """

    def verrijk(self, tekst: str, max_tekens: int = 4000,
                context: Optional[str] = None,
                image_pad: Optional[str] = None) -> VerrijkingResultaat:
        fragment = tekst[:max_tekens]
        if context:
            fragment = (
                "[CONTEXT VORIG FRAGMENT:]\n" + context[:500]
                + "\n\n[HUIDIGE ENTRY:]\n" + fragment
            )

        # Lees afbeelding als beschikbaar
        image_data = None
        if image_pad:
            image_data = _lees_en_resize_afbeelding(image_pad)

        try:
            raw = self._roep_llm_aan(fragment, image_data=image_data)
            return self._parse(raw)
        except Exception as e:
            log.warning("Verrijking mislukt: %s", e)
            return VerrijkingResultaat(fout=str(e))

    def _roep_llm_aan(self, tekst: str,
                      image_data: Optional[tuple[str, str]] = None) -> str:
        raise NotImplementedError

    def _parse(self, raw: str) -> VerrijkingResultaat:
        # Verwijder eventuele markdown-fences
        raw = re.sub(r"^```[a-z]*\n?", "", raw.strip())
        raw = re.sub(r"\n?```$", "", raw.strip())
        try:
            data = json.loads(raw)
        except json.JSONDecodeError as e:
            return VerrijkingResultaat(fout=f"JSON-parse fout: {e}\nRaw: {raw[:200]}")

        return VerrijkingResultaat(
            type=data.get("type", "onbekend"),
            samenvatting=data.get("samenvatting", ""),
            taal=data.get("taal", "nl"),
            themas=data.get("themas", []),
            emotionele_toon=data.get("emotionele_toon", "neutraal"),
            actoren=data.get("actoren", []),
            narratief=data.get("narratief", {}),
            scriptwriter=data.get("scriptwriter", {}),
            datering_hints=data.get("datering_hints", {}),
            achttienplusinhoud=bool(data.get("18plus", False)),
        )


class AnthropicVerrijker(Verrijker):
    """Gebruikt de Anthropic API (Claude). Ondersteunt vision voor afbeeldingen."""

    def __init__(self, model: str = "claude-haiku-4-5-20251001",
                 api_key: Optional[str] = None):
        self.model = model
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")

    def _roep_llm_aan(self, tekst: str,
                      image_data: Optional[tuple[str, str]] = None) -> str:
        import anthropic
        client = anthropic.Anthropic(api_key=self.api_key)

        if image_data:
            b64, mime = image_data
            content = [
                {
                    "type": "image",
                    "source": {
                        "type": "base64",
                        "media_type": mime,
                        "data": b64,
                    },
                },
            ]
            if tekst.strip():
                content.append({"type": "text", "text": tekst})
            else:
                content.append({
                    "type": "text",
                    "text": "Analyseer deze afbeelding. Als er handgeschreven "
                            "of gedrukte tekst op staat, lees die eerst volledig.",
                })
            max_tokens = 2048
        else:
            content = tekst
            max_tokens = 1024

        bericht = client.messages.create(
            model=self.model,
            max_tokens=max_tokens,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": content}],
        )
        return bericht.content[0].text


class OllamaVerrijker(Verrijker):
    """Gebruikt een lokale Ollama-instantie. Ondersteunt vision voor afbeeldingen."""

    def __init__(self, model: str = "gemma3:12b",
                 base_url: str = "http://localhost:11434"):
        self.model = model
        self.base_url = base_url

    def _roep_llm_aan(self, tekst: str,
                      image_data: Optional[tuple[str, str]] = None) -> str:
        import urllib.request

        prompt_tekst = SYSTEM_PROMPT + "\n\nDocument:\n" + (tekst or "")
        if not tekst.strip() and image_data:
            prompt_tekst = (
                SYSTEM_PROMPT + "\n\nAnalyseer deze afbeelding. Als er "
                "handgeschreven of gedrukte tekst op staat, lees die eerst volledig."
            )

        payload_dict = {
            "model": self.model,
            "prompt": prompt_tekst,
            "stream": False,
            "options": {
                "num_thread": 10,
                "num_gpu": 99,
                "num_ctx": 8192,
            },
        }

        # Ollama multimodal: voeg base64-afbeeldingen toe
        if image_data:
            b64, _mime = image_data
            payload_dict["images"] = [b64]

        payload = json.dumps(payload_dict).encode()
        req = urllib.request.Request(
            f"{self.base_url}/api/generate",
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=120) as resp:
            data = json.loads(resp.read())
        return data.get("response", "")


def maak_verrijker(backend: str = "anthropic", **kwargs) -> Verrijker:
    """
    Fabrieksfunctie. backend = 'anthropic' | 'ollama'
    """
    if backend == "ollama":
        return OllamaVerrijker(**kwargs)
    return AnthropicVerrijker(**kwargs)
