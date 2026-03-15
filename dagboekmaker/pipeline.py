"""
dagboekmaker.pipeline
~~~~~~~~~~~~~~~~~~~~~
Hoofd-orkestrator die bronbestanden verwerkt tot verrijkte corpus-documenten.

Gebruik:
    from dagboekmaker.pipeline import Pipeline

    p = Pipeline(
        bronmap="/pad/naar/archief",
        corpusmap="/pad/naar/output",
        backend="anthropic",    # of "ollama"
    )
    p.verwerk_alles()
"""

import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .corpus import Corpus
from .datering import dateer_lokaal, GlobaleDateringsmotor
from .extractor import extraheer, EXTENSIONS
from .verrijking import maak_verrijker

log = logging.getLogger(__name__)


def _maak_doc_id(pad: str) -> str:
    """Stabiel, deterministisch ID op basis van bestandspad."""
    return "doc_" + hashlib.sha1(pad.encode()).hexdigest()[:12]


class Pipeline:
    def __init__(
        self,
        bronmap: str,
        corpusmap: str,
        backend: str = "anthropic",
        verrijker_kwargs: Optional[dict] = None,
        herverwerk: bool = False,
    ):
        self.bronmap = Path(bronmap)
        self.corpus = Corpus(corpusmap)
        self.verrijker = maak_verrijker(backend, **(verrijker_kwargs or {}))
        self.herverwerk = herverwerk

    # ── Publieke API ─────────────────────────────────────────────────────────

    def verwerk_alles(self, glob: str = "**/*"):
        """Verwerkt alle bronbestanden recursief."""
        bestanden = [
            p for p in self.bronmap.glob(glob)
            if p.is_file() and p.suffix.lower() in EXTENSIONS
        ]
        log.info("Gevonden: %d bronbestanden", len(bestanden))
        verwerkt = 0
        fouten = 0

        for pad in bestanden:
            try:
                doc_id = self.verwerk_bestand(str(pad))
                if doc_id:
                    verwerkt += 1
            except Exception as e:
                log.error("Fout bij %s: %s", pad, e)
                fouten += 1

        log.info("Klaar: %d verwerkt, %d fouten", verwerkt, fouten)
        self._fase2_globaal()
        self.corpus.exporteer_actors_json()
        self._exporteer_dashboard_data()

    def verwerk_bestand(self, pad: str) -> Optional[str]:
        """Verwerkt één bestand. Geeft doc_id terug of None als overgeslagen."""
        doc_id = _maak_doc_id(pad)

        if not self.herverwerk:
            bestaand = self.corpus.haal_document_op(doc_id)
            if bestaand:
                log.debug("Overgeslagen (al verwerkt): %s", pad)
                return doc_id

        # Stap 1: extractie
        extractie = extraheer(pad)
        if extractie.fout and not extractie.tekst:
            log.warning("Extractie mislukt: %s — %s", pad, extractie.fout)
            return None

        tekst = extractie.tekst or ""

        # Stap 2: datering fase 1
        datum = dateer_lokaal(
            tekst,
            bestandsdatum=extractie.bestandsdatum,
            exif=extractie.exif or {},
        )

        # Stap 3: verrijking via LLM
        verrijking = self.verrijker.verrijk(tekst)

        # Integreer datering_hints uit LLM in redenering
        if verrijking.datering_hints:
            hints = verrijking.datering_hints
            for citaat in hints.get("expliciete_vermeldingen", []):
                datum.redenering.append({
                    "type": "llm_hint_expliciet",
                    "bewijs": citaat,
                    "gewicht": 0.6,
                })
            for cultuur in hints.get("cultuurverwijzingen", []):
                datum.redenering.append({
                    "type": "llm_hint_cultuur",
                    "bewijs": cultuur,
                    "gewicht": 0.4,
                })

        # Stap 4: stel levensperiode in
        levensperiode = self._bepaal_levensperiode(datum.datum_geschat)

        # Stap 5: bouw document-dict
        actors = _normaliseer_actoren(verrijking.actoren)
        doc = {
            "id": doc_id,
            "tijdstip": datum.als_dict(),
            "type": verrijking.type,
            "formaat_origineel": extractie.formaat,
            "bestand_origineel": pad,
            "inhoud": {
                "plaintext": tekst[:50_000],  # max 50k tekens opslaan
                "samenvatting": verrijking.samenvatting,
                "taal": verrijking.taal,
                "themas": verrijking.themas,
                "emotionele_toon": verrijking.emotionele_toon,
                "type": verrijking.type,
            },
            "actors": actors,
            "narratief": {
                **verrijking.narratief,
                "levensperiode": levensperiode,
            },
            "levensperiode": levensperiode,
            "financieel": None,
            "verwerkings_meta": {
                "verwerkt_op": datetime.now(tz=timezone.utc).isoformat(),
                "tool": "dagboekmaker v0.1",
                "extractor_methode": extractie.methode,
                "extractie_fout": extractie.fout,
                "bestandsdatum": extractie.bestandsdatum,
            },
        }

        self.corpus.sla_document_op(doc)
        log.info("Verwerkt: %s → %s (%s)", Path(pad).name, doc_id,
                 datum.datum_geschat or "?")
        return doc_id

    # ── Fase 2: globale constraints ──────────────────────────────────────────

    def _fase2_globaal(self):
        """Past actorconstraints en reeks-priors toe over alle documenten."""
        log.info("Fase 2: globale dateringsconstraints...")
        actors = self.corpus.haal_alle_actors_op()
        motor = GlobaleDateringsmotor(actors)

        alle_docs = self.corpus.zoek()
        for doc in alle_docs:
            if not doc:
                continue
            d_dict = doc.get("tijdstip", {})
            if d_dict.get("zekerheid", 0) >= 0.9:
                continue  # al voldoende zeker

            from .datering import DatumOnzekerheid
            d = _dict_naar_datum(d_dict)
            doc_actors = doc.get("actors", [])
            d = motor.pas_actor_constraints_toe(doc["id"], doc_actors, d)

            doc["tijdstip"] = d.als_dict()
            doc["levensperiode"] = self._bepaal_levensperiode(d.datum_geschat)
            doc["narratief"]["levensperiode"] = doc["levensperiode"]
            self.corpus.sla_document_op(doc)

        log.info("Fase 2 klaar.")

    # ── Dashboard-export ─────────────────────────────────────────────────────

    def _exporteer_dashboard_data(self):
        dichtheid = self.corpus.tijdlijn_dichtheid()
        stats = self.corpus.voortgang_stats()
        pad = Path(self.corpus.root) / "dashboard_data.json"
        pad.write_text(json.dumps({
            "dichtheid": dichtheid,
            "voortgang": stats,
            "gegenereerd_op": datetime.now(tz=timezone.utc).isoformat(),
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        log.info("Dashboard-data geschreven naar %s", pad)

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _bepaal_levensperiode(self, datum_geschat: Optional[str]) -> Optional[str]:
        if not datum_geschat:
            return None
        import re
        m = re.search(r"\d{4}", datum_geschat)
        if not m:
            return None
        jaar = int(m.group())
        periodes = [
            ("kindertijd",     None, 1985),
            ("adolescentie",   1985, 1993),
            ("jong_volwassen", 1993, 2002),
            ("breekpunt",      2002, 2005),
            ("opbouw",         2005, 2015),
            ("heden",          2015, None),
        ]
        for naam, van, tot in periodes:
            if (van is None or jaar >= van) and (tot is None or jaar < tot):
                return naam
        return None


# ── Hulpfuncties ──────────────────────────────────────────────────────────────

def _normaliseer_actoren(actoren: list) -> list:
    """Maak actor-refs van LLM-output (namen → IDs)."""
    result = []
    for a in actoren:
        naam = a.get("naam", "").strip()
        if not naam:
            continue
        actor_id = "actor_" + hashlib.sha1(naam.lower().encode()).hexdigest()[:8]
        result.append({"ref": actor_id, "rol": a.get("rol", "vermeld"),
                        "_naam_origineel": naam})
    return result


def _dict_naar_datum(d: dict):
    from .datering import DatumOnzekerheid
    import re
    du = DatumOnzekerheid()
    du.zekerheid = d.get("zekerheid", 0.0)
    du.datum_geschat = d.get("datum_geschat")
    du.redenering = d.get("redenering", [])
    du.geschiedenis = d.get("daterings_geschiedenis", [])
    vroegst = d.get("datum_vroegst", "")
    laatst  = d.get("datum_laatst", "")
    m = re.match(r"(\d{4})", vroegst or "")
    if m:
        du.jaar_min = int(m.group(1))
    m = re.match(r"(\d{4})", laatst or "")
    if m:
        du.jaar_max = int(m.group(1))
    return du
