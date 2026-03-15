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
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .corpus import Corpus
from .datering import dateer_lokaal, GlobaleDateringsmotor
from .extractor import extraheer, EXTENSIONS, _sniff
from .splitter import splits_dagboek
from .verrijking import maak_verrijker

log = logging.getLogger(__name__)


def _maak_doc_id(pad: str) -> str:
    """Stabiel, deterministisch ID op basis van bestandspad."""
    return "doc_" + hashlib.sha1(pad.encode()).hexdigest()[:12]


def _maak_fragment_id(pad: str, volgnummer: int) -> str:
    """Deterministisch ID voor een fragment uit een bronbestand.

    Gebruikt als fallback wanneer nog geen datum/type bekend is.
    Gebruik _maak_leesbaar_fragment_id() voor de definitieve ID na verrijking.
    """
    basis = f"{pad}::frag_{volgnummer}"
    return "doc_" + hashlib.sha1(basis.encode()).hexdigest()[:12]


def _maak_leesbaar_fragment_id(datum_geschat: Optional[str],
                                doc_type: str = "dagboek",
                                pad: str = "",
                                volgnummer: int = 0) -> str:
    """Leesbaar ID op basis van datum en type, bv. doc_19950314_dagboek.

    Bij dubbele datums wordt een suffix (_2, _3, ...) toegevoegd op basis
    van het volgnummer in het bronbestand.
    """
    import re
    # Extraheer YYYYMMDD uit datum_geschat
    datum_deel = "00000000"
    if datum_geschat:
        # Probeer ISO-formaat: "1995-03-14", "1995-03", "1995"
        cijfers = re.findall(r"\d+", datum_geschat)
        if cijfers:
            jaar = cijfers[0].zfill(4)
            maand = cijfers[1].zfill(2) if len(cijfers) > 1 else "00"
            dag = cijfers[2].zfill(2) if len(cijfers) > 2 else "00"
            datum_deel = f"{jaar}{maand}{dag}"

    # Maak type URL-veilig
    type_deel = re.sub(r"[^a-z0-9]", "_", doc_type.lower().strip())[:20]

    basis_id = f"doc_{datum_deel}_{type_deel}"

    # Voeg volgnummer toe als disambiguatie (altijd, voor determinisme)
    if volgnummer > 0:
        basis_id += f"_{volgnummer}"

    return basis_id


class Pipeline:
    def __init__(
        self,
        bronmap: str,
        corpusmap: str,
        backend: str = "ollama",
        verrijker_kwargs: Optional[dict] = None,
        herverwerk: bool = False,
        tweede_pass: bool = True,
        split_dagboeken: bool = True,
    ):
        self.bronmap = Path(bronmap)
        self.corpus = Corpus(corpusmap)
        self.verrijker = maak_verrijker(backend, **(verrijker_kwargs or {}))
        self.herverwerk = herverwerk
        self.tweede_pass = tweede_pass
        self.split_dagboeken = split_dagboeken
        # Tweede pass altijd met Claude
        if tweede_pass and backend != "anthropic":
            self._verrijker_claude = maak_verrijker("anthropic")
        else:
            self._verrijker_claude = None

    # ── Publieke API ─────────────────────────────────────────────────────────

    def verwerk_alles(self, glob: str = "**/*",
                      voortgang_callback=None,
                      stop_event=None,
                      pause_event=None):
        """Verwerkt alle bronbestanden recursief.

        voortgang_callback: optional callable receiving progress dict
        stop_event: threading.Event — if set, stops after current file
        pause_event: threading.Event — if cleared, blocks until set again
        """
        bestanden = [
            p for p in self.bronmap.glob(glob)
            if p.is_file()
            and (p.suffix.lower() in EXTENSIONS
                 or _sniff(p) is not None)
        ]
        totaal = len(bestanden)
        log.info("Gevonden: %d bronbestanden", totaal)
        verwerkt = 0
        fouten = 0

        for i, pad in enumerate(bestanden):
            # Cooperative stop
            if stop_event and stop_event.is_set():
                log.info("Stop gevraagd, afgebroken na %d bestanden", verwerkt)
                break

            # Cooperative pause
            if pause_event and not pause_event.is_set():
                if voortgang_callback:
                    voortgang_callback({"fase": "gepauzeerd", "nummer": i, "totaal": totaal})
                pause_event.wait()

            if voortgang_callback:
                voortgang_callback({
                    "bestand": str(pad.name),
                    "fase": "verwerking",
                    "nummer": i + 1,
                    "totaal": totaal,
                })

            try:
                doc_ids = self.verwerk_bestand(str(pad))
                if doc_ids:
                    verwerkt += len(doc_ids)
            except Exception as e:
                log.error("Fout bij %s: %s", pad, e)
                fouten += 1
                if voortgang_callback:
                    voortgang_callback({
                        "bestand": str(pad.name),
                        "fase": "fout",
                        "fout": str(e),
                        "nummer": i + 1,
                        "totaal": totaal,
                    })

        # ── Tweede pass: Claude-verfijning ──
        if self._verrijker_claude and verwerkt > 0:
            if stop_event and stop_event.is_set():
                log.info("Stop gevraagd, tweede pass overgeslagen")
            else:
                log.info("Tweede pass: Claude-verfijning van %d documenten...", verwerkt)
                if voortgang_callback:
                    voortgang_callback({"fase": "claude_pass", "nummer": 0, "totaal": verwerkt})
                self._tweede_pass_claude(
                    voortgang_callback=voortgang_callback,
                    stop_event=stop_event,
                    pause_event=pause_event,
                    totaal=verwerkt,
                )

        if voortgang_callback:
            voortgang_callback({"fase": "fase2", "nummer": verwerkt, "totaal": totaal})

        log.info("Klaar: %d verwerkt, %d fouten", verwerkt, fouten)
        self._fase2_globaal()
        self.corpus.exporteer_actors_json()
        self._exporteer_dashboard_data()

    def verwerk_bestand(self, pad: str) -> Optional[list[str]]:
        """Verwerkt één bestand. Geeft lijst van doc_ids terug of None als overgeslagen."""
        doc_id = _maak_doc_id(pad)

        if not self.herverwerk:
            bestaand = self.corpus.haal_document_op(doc_id)
            if bestaand:
                log.debug("Overgeslagen (al verwerkt): %s", pad)
                return [doc_id]

        # Stap 1: extractie
        extractie = extraheer(pad)
        if extractie.fout and not extractie.tekst:
            log.warning("Extractie mislukt: %s — %s", pad, extractie.fout)
            return None

        tekst = extractie.tekst or ""

        # Stap 2: splits op datumkoppen (als ingeschakeld)
        if self.split_dagboeken:
            split = splits_dagboek(tekst)
        else:
            split = None

        if split and split.is_gesplitst:
            return self._verwerk_fragmenten(pad, extractie, split)

        # Geen splitsing: bestaande logica voor enkelvoudig document
        return self._verwerk_enkel(pad, doc_id, extractie, tekst)

    def _verwerk_enkel(self, pad: str, doc_id: str, extractie, tekst: str) -> list[str]:
        """Verwerkt een enkel (niet-gesplitst) document."""
        datum = dateer_lokaal(
            tekst,
            bestandsdatum=extractie.bestandsdatum,
            exif=extractie.exif or {},
        )
        verrijking = self.verrijker.verrijk(tekst)
        self._integreer_datering_hints(datum, verrijking)

        levensperiode = self._bepaal_levensperiode(datum.datum_geschat)
        actors = _normaliseer_actoren(verrijking.actoren)
        doc = {
            "id": doc_id,
            "tijdstip": datum.als_dict(),
            "type": verrijking.type,
            "formaat_origineel": extractie.formaat,
            "bestand_origineel": pad,
            "inhoud": {
                "plaintext": tekst[:50_000],
                "samenvatting": verrijking.samenvatting,
                "taal": verrijking.taal,
                "themas": verrijking.themas,
                "emotionele_toon": verrijking.emotionele_toon,
                "type": verrijking.type,
                "18plus": verrijking.achttienplusinhoud,
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
                "tool": "dagboekmaker v0.2",
                "extractor_methode": extractie.methode,
                "extractie_fout": extractie.fout,
                "bestandsdatum": extractie.bestandsdatum,
            },
        }
        self.corpus.sla_document_op(doc)
        log.info("Verwerkt: %s → %s (%s)", Path(pad).name, doc_id,
                 datum.datum_geschat or "?")
        return [doc_id]

    def _verwerk_fragmenten(self, pad: str, extractie, split) -> list[str]:
        """Verwerkt een gesplitst bronbestand: elk fragment apart.

        Twee-pass aanpak:
          1. Dateer + verrijk elk fragment (nodig om leesbare IDs te maken)
          2. Genereer leesbare IDs, bouw docs met serie-links, sla op
        """
        bron_id = _maak_doc_id(pad)
        fragmenten = split.fragmenten
        totaal = len(fragmenten)

        # Verwijder eventueel bestaand enkelvoudig document (herverwerk-scenario)
        if self.herverwerk:
            bestaand = self.corpus.haal_document_op(bron_id)
            if bestaand and not bestaand.get("serie"):
                self.corpus.verwijder_document(bron_id)
            # Verwijder ook bestaande fragmenten bij herverwerking
            bestaande_serie = self.corpus.haal_serie_op(bron_id)
            for oud_doc in bestaande_serie:
                if oud_doc:
                    self.corpus.verwijder_document(oud_doc["id"])

        # Pass 1: dateer + verrijk elk fragment
        verwerkte = []  # lijst van (frag, datum, verrijking)
        vorig_tekst = None
        for frag in fragmenten:
            datum = dateer_lokaal(
                frag.tekst,
                bestandsdatum=extractie.bestandsdatum,
                exif=extractie.exif or {},
            )
            verrijking = self.verrijker.verrijk(frag.tekst, context=vorig_tekst)
            self._integreer_datering_hints(datum, verrijking)
            verwerkte.append((frag, datum, verrijking))
            vorig_tekst = frag.tekst[-500:]

        # Pass 2: genereer leesbare IDs en bouw docs
        frag_ids = []
        for frag, datum, verrijking in verwerkte:
            frag_id = _maak_leesbaar_fragment_id(
                datum_geschat=datum.datum_geschat,
                doc_type=verrijking.type,
                pad=pad,
                volgnummer=frag.volgnummer,
            )
            frag_ids.append(frag_id)

        doc_ids = []
        for i, (frag, datum, verrijking) in enumerate(verwerkte):
            frag_id = frag_ids[i]
            levensperiode = self._bepaal_levensperiode(datum.datum_geschat)
            actors = _normaliseer_actoren(verrijking.actoren)

            doc = {
                "id": frag_id,
                "tijdstip": datum.als_dict(),
                "type": verrijking.type,
                "formaat_origineel": extractie.formaat,
                "bestand_origineel": pad,
                "serie": {
                    "bron_id": bron_id,
                    "bron_bestand": pad,
                    "volgnummer": frag.volgnummer,
                    "totaal": totaal,
                    "vorige_id": frag_ids[i - 1] if i > 0 else None,
                    "volgende_id": frag_ids[i + 1] if i + 1 < totaal else None,
                    "datum_header_ruw": frag.datum_header,
                    "is_proloog": frag.datum_header is None,
                },
                "inhoud": {
                    "plaintext": frag.tekst[:50_000],
                    "samenvatting": verrijking.samenvatting,
                    "taal": verrijking.taal,
                    "themas": verrijking.themas,
                    "emotionele_toon": verrijking.emotionele_toon,
                    "type": verrijking.type,
                    "18plus": verrijking.achttienplusinhoud,
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
                    "tool": "dagboekmaker v0.2",
                    "extractor_methode": extractie.methode,
                    "extractie_fout": extractie.fout,
                    "bestandsdatum": extractie.bestandsdatum,
                    "split_methode": split.methode,
                    "fragment_van_totaal": f"{frag.volgnummer + 1}/{totaal}",
                },
            }

            self.corpus.sla_document_op(doc)
            log.info("Verwerkt: %s frag %d/%d → %s (%s)",
                     Path(pad).name, frag.volgnummer + 1, totaal,
                     frag_id, datum.datum_geschat or "?")
            doc_ids.append(frag_id)

        return doc_ids

    @staticmethod
    def _integreer_datering_hints(datum, verrijking):
        """Verwerkt LLM datering_hints: past zekerheid en jaarbereik aan."""
        if not verrijking.datering_hints:
            return
        hints = verrijking.datering_hints

        # Expliciete vermeldingen — probeer jaartallen te extraheren
        for citaat in hints.get("expliciete_vermeldingen", []):
            jaren = re.findall(r"\b((?:19|20)\d{2})\b", citaat)
            if jaren:
                gevonden = [int(j) for j in jaren]
                nieuw_min = max(datum.jaar_min, min(gevonden))
                nieuw_max = min(datum.jaar_max, max(gevonden))
                if nieuw_min <= nieuw_max:
                    datum.jaar_min = nieuw_min
                    datum.jaar_max = nieuw_max
                    datum.zekerheid = max(datum.zekerheid, 0.75)
                    datum.redenering.append({
                        "type": "llm_hint_expliciet",
                        "bewijs": citaat,
                        "gewicht": 0.75,
                        "effect": f"jaar beperkt tot {nieuw_min}–{nieuw_max}",
                    })
                    datum._log_versie("llm_hint_expliciet",
                                      f"Jaar {nieuw_min}–{nieuw_max} via '{citaat[:60]}'")
                    continue
            datum.redenering.append({
                "type": "llm_hint_expliciet",
                "bewijs": citaat,
                "gewicht": 0.6,
            })

        # Cultuurverwijzingen — probeer jaartallen te extraheren
        for cultuur in hints.get("cultuurverwijzingen", []):
            jaren = re.findall(r"\b((?:19|20)\d{2})\b", cultuur)
            if jaren:
                gevonden = [int(j) for j in jaren]
                nieuw_min = max(datum.jaar_min, min(gevonden) - 1)
                nieuw_max = min(datum.jaar_max, max(gevonden) + 2)
                if nieuw_min <= nieuw_max:
                    datum.jaar_min = nieuw_min
                    datum.jaar_max = nieuw_max
                    datum.zekerheid = max(datum.zekerheid, 0.55)
                    datum.redenering.append({
                        "type": "llm_hint_cultuur",
                        "bewijs": cultuur,
                        "gewicht": 0.55,
                        "effect": f"jaar beperkt tot {nieuw_min}–{nieuw_max}",
                    })
                    datum._log_versie("llm_hint_cultuur",
                                      f"Jaar {nieuw_min}–{nieuw_max} via '{cultuur[:60]}'")
                    continue
            datum.redenering.append({
                "type": "llm_hint_cultuur",
                "bewijs": cultuur,
                "gewicht": 0.4,
            })

        # Leeftijdsverwijzingen — schat geboorte ~1975 (auteur)
        AUTEUR_GEBOORTEJAAR = 1975
        for leeftijd_hint in hints.get("leeftijdsverwijzingen", []):
            m = re.search(r"(\d{1,2})\s*(?:jaar|jarig|word|wordt)", leeftijd_hint)
            if m:
                leeftijd = int(m.group(1))
                geschat_jaar = AUTEUR_GEBOORTEJAAR + leeftijd
                nieuw_min = max(datum.jaar_min, geschat_jaar - 1)
                nieuw_max = min(datum.jaar_max, geschat_jaar + 1)
                if nieuw_min <= nieuw_max:
                    datum.jaar_min = nieuw_min
                    datum.jaar_max = nieuw_max
                    datum.zekerheid = max(datum.zekerheid, 0.6)
                    datum.redenering.append({
                        "type": "llm_hint_leeftijd",
                        "bewijs": leeftijd_hint,
                        "gewicht": 0.6,
                        "effect": f"leeftijd {leeftijd} → circa {geschat_jaar}",
                    })
                    datum._log_versie("llm_hint_leeftijd",
                                      f"Leeftijd {leeftijd} → ~{geschat_jaar}")

        # Update schatting na hints
        from .datering import _stel_schatting_in
        _stel_schatting_in(datum)

    # ── Tweede pass: Claude-verfijning ───────────────────────────────────────

    def _tweede_pass_claude(self, voortgang_callback=None, stop_event=None,
                            pause_event=None, totaal=0):
        """Herverrijkt alle documenten via Claude voor diepere analyse."""
        alle_docs = self.corpus.zoek()
        for i, doc in enumerate(alle_docs):
            if not doc:
                continue

            if stop_event and stop_event.is_set():
                log.info("Stop gevraagd, Claude-pass afgebroken na %d docs", i)
                break

            if pause_event and not pause_event.is_set():
                if voortgang_callback:
                    voortgang_callback({"fase": "gepauzeerd (claude)", "nummer": i, "totaal": totaal})
                pause_event.wait()

            if voortgang_callback:
                voortgang_callback({
                    "bestand": doc.get("bestand_origineel", doc["id"]),
                    "fase": "claude_pass",
                    "nummer": i + 1,
                    "totaal": totaal,
                })

            tekst = doc.get("inhoud", {}).get("plaintext", "")
            if not tekst:
                continue

            try:
                verrijking = self._verrijker_claude.verrijk(tekst)
                if verrijking.fout:
                    log.warning("Claude-pass fout voor %s: %s", doc["id"], verrijking.fout)
                    continue

                # Merge: Claude overschrijft, maar behoud wat Ollama al goed had
                inhoud = doc.get("inhoud", {})
                inhoud["samenvatting"] = verrijking.samenvatting or inhoud.get("samenvatting", "")
                inhoud["type"] = verrijking.type or inhoud.get("type", "onbekend")
                inhoud["themas"] = verrijking.themas or inhoud.get("themas", [])
                inhoud["emotionele_toon"] = verrijking.emotionele_toon or inhoud.get("emotionele_toon")
                doc["inhoud"] = inhoud
                doc["type"] = verrijking.type or doc.get("type", "onbekend")

                # Merge actoren (voeg nieuwe toe, behoud bestaande)
                bestaande_namen = {a.get("_naam_origineel", "").lower() for a in doc.get("actors", [])}
                for a in _normaliseer_actoren(verrijking.actoren):
                    if a.get("_naam_origineel", "").lower() not in bestaande_namen:
                        doc.setdefault("actors", []).append(a)

                # Narratief verfijnen
                if verrijking.narratief:
                    doc["narratief"] = {**doc.get("narratief", {}), **verrijking.narratief}

                # Datering hints toevoegen
                if verrijking.datering_hints:
                    meta = doc.get("verwerkings_meta", {})
                    meta["claude_pass"] = datetime.now(tz=timezone.utc).isoformat()
                    doc["verwerkings_meta"] = meta

                self.corpus.sla_document_op(doc)
                log.debug("Claude-pass: %s verfijnd", doc["id"])

            except Exception as e:
                log.warning("Claude-pass fout %s: %s", doc["id"], e)

        log.info("Claude-pass klaar.")

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
    """Maak actor-refs van LLM-output (namen → IDs). Ondersteunt meervoudige rollen."""
    # Groepeer per naam zodat dezelfde actor meerdere rollen kan krijgen
    per_naam = {}
    for a in actoren:
        naam = a.get("naam", "").strip()
        if not naam:
            continue
        sleutel = naam.lower()
        rol = a.get("rol", "vermeld")
        if sleutel not in per_naam:
            per_naam[sleutel] = {"naam": naam, "rollen": set()}
        per_naam[sleutel]["rollen"].add(rol)

    result = []
    for sleutel, info in per_naam.items():
        actor_id = "actor_" + hashlib.sha1(sleutel.encode()).hexdigest()[:8]
        rollen = sorted(info["rollen"])
        result.append({
            "ref": actor_id,
            "rol": rollen[0] if len(rollen) == 1 else rollen,
            "_naam_origineel": info["naam"],
        })
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
