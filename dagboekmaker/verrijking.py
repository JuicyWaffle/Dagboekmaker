"""
dagboekmaker.verrijking
~~~~~~~~~~~~~~~~~~~~~~~
Verrijkt een geëxtraheerd document met:
  - documenttype
  - samenvatting
  - thema's
  - emotionele toon
  - actoren (namen + aliassen)
  - levensperiode-indeling
  - narratieve spanning

Kan draaien met de Anthropic API (Claude) of een lokale Ollama-instantie.
"""

import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger(__name__)

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

{
  "type": "<één van: dagboek, brief, notitie, foto, ansichtkaart, e-mail, sms, chat, fax, rapport, memo, notulen, presentatie, cv, sollicitatie, financieel, factuur, contract, akte, belasting, verzekering, paspoort, rijbewijs, diploma, certificaat, attest, medisch, recept, gedicht, verhaal, essay, manuscript, scenario, artikel, knipsel, blog, forumpost, recept_koken, boodschappenlijst, to-do, code, configuratie, logbestand, onbekend>",
  "samenvatting": "<maximaal 2 zinnen, feitelijk — wees eerlijk over de inhoud, ook als die intiem is>",
  "taal": "<ISO 639-1 taalcode, bv 'nl', 'fr', 'en'>",
  "themas": ["<thema1>", "<thema2>"],
  "emotionele_toon": "<één van: blij, verdrietig, bezorgd, boos, nostalgisch, neutraal, hoopvol, angstig, ironisch, liefdevol>",
  "18plus": <true als het document seksuele, erotische of expliciet intieme inhoud bevat, anders false>,
  "actoren": [
    {"naam": "<naam zoals in document>", "rol": "<auteur|ontvanger|vermeld>"}
  ],
  "narratief": {
    "spanning": <0.0–1.0, hoger = dramatisch belangrijker>,
    "keerpunt": <true|false>,
    "notitie": "<optionele redactionele noot voor scriptschrijver>"
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
    datering_hints:   dict = field(default_factory=dict)
    achttienplusinhoud: bool = False
    fout:             Optional[str] = None


class Verrijker:
    """
    Abstracte verrijker. Gebruik AnthropicVerrijker of OllamaVerrijker.
    """

    def verrijk(self, tekst: str, max_tekens: int = 4000,
                context: Optional[str] = None) -> VerrijkingResultaat:
        fragment = tekst[:max_tekens]
        if context:
            fragment = (
                "[CONTEXT VORIG FRAGMENT:]\n" + context[:500]
                + "\n\n[HUIDIGE ENTRY:]\n" + fragment
            )
        try:
            raw = self._roep_llm_aan(fragment)
            return self._parse(raw)
        except Exception as e:
            log.warning("Verrijking mislukt: %s", e)
            return VerrijkingResultaat(fout=str(e))

    def _roep_llm_aan(self, tekst: str) -> str:
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
            datering_hints=data.get("datering_hints", {}),
            achttienplusinhoud=bool(data.get("18plus", False)),
        )


class AnthropicVerrijker(Verrijker):
    """Gebruikt de Anthropic API (claude-3-haiku voor snelheid/kosten)."""

    def __init__(self, model: str = "claude-haiku-4-5-20251001",
                 api_key: Optional[str] = None):
        self.model = model
        self.api_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")

    def _roep_llm_aan(self, tekst: str) -> str:
        import anthropic
        client = anthropic.Anthropic(api_key=self.api_key)
        bericht = client.messages.create(
            model=self.model,
            max_tokens=1024,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": tekst}],
        )
        return bericht.content[0].text


class OllamaVerrijker(Verrijker):
    """Gebruikt een lokale Ollama-instantie."""

    def __init__(self, model: str = "gemma3:12b",
                 base_url: str = "http://localhost:11434"):
        self.model = model
        self.base_url = base_url

    def _roep_llm_aan(self, tekst: str) -> str:
        import urllib.request
        payload = json.dumps({
            "model": self.model,
            "prompt": SYSTEM_PROMPT + "\n\nDocument:\n" + tekst,
            "stream": False,
            "options": {
                "num_thread": 10,     # alle CPU-cores
                "num_gpu": 99,        # alle lagen op GPU (Metal)
                "num_ctx": 8192,      # ruim contextvenster
            },
        }).encode()
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
