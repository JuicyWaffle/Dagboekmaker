"""
dagboekmaker.datering
~~~~~~~~~~~~~~~~~~~~~
Twee-fase dateringsmotor.

Fase 1 – Lokaal:  regex op tekst, bestandsdatum, kalendercheck
Fase 2 – Globaal: actorconstraints, documentreeks, LLM-analyse
"""

import re
import json
import logging
from dataclasses import dataclass, field
from typing import Optional
from datetime import date, datetime

log = logging.getLogger(__name__)

# Dag-van-week namen (NL)
_DOW_NL = {
    "maandag": 0, "dinsdag": 1, "woensdag": 2, "donderdag": 3,
    "vrijdag": 4, "zaterdag": 5, "zondag": 6,
    "monday": 0, "tuesday": 1, "wednesday": 2, "thursday": 3,
    "friday": 4, "saturday": 5, "sunday": 6,
}

_MAAND_NL = {
    "januari": 1, "februari": 2, "maart": 3, "april": 4,
    "mei": 5, "juni": 6, "juli": 7, "augustus": 8,
    "september": 9, "oktober": 10, "november": 11, "december": 12,
    "january": 1, "february": 2, "march": 3, "may": 5,
    "june": 6, "july": 7, "august": 8, "october": 10,
}

# Beschikbaar jaarbereik voor de corpus
JAAR_MIN = 1970
JAAR_MAX = 2025


@dataclass
class DatumOnzekerheid:
    dag:             Optional[int]  = None   # 1–31
    maand:           Optional[int]  = None   # 1–12
    jaar_min:        int            = JAAR_MIN
    jaar_max:        int            = JAAR_MAX
    jaar_kandidaten: list           = field(default_factory=list)
    zekerheid:       float          = 0.0
    dag_van_week:    Optional[str]  = None
    datum_geschat:   Optional[str]  = None   # ISO fragmentstring, bv "1995-03"
    redenering:      list           = field(default_factory=list)
    geschiedenis:    list           = field(default_factory=list)

    def is_opgelost(self) -> bool:
        return (self.jaar_min == self.jaar_max and
                self.maand is not None and
                self.zekerheid >= 0.85)

    def als_dict(self) -> dict:
        return {
            "datum_vroegst":  f"{self.jaar_min}-{self.maand or 1:02d}-{self.dag or 1:02d}",
            "datum_laatst":   f"{self.jaar_max}-{self.maand or 12:02d}-{self.dag or 28:02d}",
            "datum_geschat":  self.datum_geschat,
            "precisie":       self._precisie(),
            "zekerheid":      round(self.zekerheid, 3),
            "dag_van_week":   self.dag_van_week,
            "redenering":     self.redenering,
            "daterings_geschiedenis": self.geschiedenis,
        }

    def _precisie(self) -> str:
        if self.jaar_min == self.jaar_max:
            if self.maand:
                return "dag" if self.dag else "maand"
            return "jaar"
        span = self.jaar_max - self.jaar_min
        if span <= 2:
            return "circa_2jaar"
        if span <= 5:
            return "circa_5jaar"
        return "onbekend"

    def _log_versie(self, methode: str, notitie: str = ""):
        versie = len(self.geschiedenis) + 1
        self.geschiedenis.append({
            "versie": versie,
            "datum_geschat": self.datum_geschat,
            "zekerheid": round(self.zekerheid, 3),
            "methode": methode,
            "notitie": notitie,
        })


# ── Fase 1: lokale datering ───────────────────────────────────────────────────

def dateer_lokaal(tekst: str, bestandsdatum: Optional[str] = None,
                  exif: Optional[dict] = None,
                  bestand_pad: Optional[str] = None) -> DatumOnzekerheid:
    """
    Probeert datum te bepalen uit tekst, bestandsdatum, EXIF en bestandsnaam.
    Geeft een DatumOnzekerheid terug, ook als er weinig info is.
    """
    d = DatumOnzekerheid()

    # 1. EXIF-datum (foto's) — hoogste zekerheid
    if exif:
        exif_datum = exif.get("DateTimeOriginal") or exif.get("CreateDate")
        if exif_datum:
            parsed = _parse_exif_datum(exif_datum)
            if parsed:
                d.dag, d.maand = parsed.day, parsed.month
                d.jaar_min = d.jaar_max = parsed.year
                d.zekerheid = 0.97
                d.datum_geschat = parsed.isoformat()
                d.redenering.append({
                    "type": "exif_datum",
                    "bewijs": f"EXIF DateTimeOriginal = {exif_datum}",
                    "gewicht": 0.97,
                })
                d._log_versie("exif")
                return d

    # 1b. Datum uit bestandsnaam (bv. "2014-01-05 15.21.16.jpg")
    if bestand_pad:
        _zoek_datum_in_bestandsnaam(bestand_pad, d)
        if d.zekerheid >= 0.85:
            return d

    # 2. Expliciete datumpatronen in tekst
    _zoek_expliciete_datum(tekst, d)

    # 3. Dag+maand zonder jaar → kalendercheck
    if d.dag and d.maand and not (d.jaar_min == d.jaar_max):
        _kalender_check(d)

    # 3b. Relatieve datums oplossen via contextankers
    _resolv_relatieve_datums(tekst, d)

    # 4. Bestandsdatum als zwakke prior
    if bestandsdatum and d.zekerheid < 0.5:
        try:
            bd = date.fromisoformat(bestandsdatum)
            # bestandsdatum is onbetrouwbaar maar geeft een bovengrens
            d.jaar_max = min(d.jaar_max, bd.year + 1)
            d.redenering.append({
                "type": "bestandsdatum",
                "bewijs": f"Bestandsdatum = {bestandsdatum} (zwakke prior, bovengrens)",
                "gewicht": 0.25,
            })
            d.zekerheid = max(d.zekerheid, 0.2)
            d._log_versie("bestandsdatum_prior",
                          f"Jaar_max beperkt tot {d.jaar_max}")
        except ValueError:
            pass

    if d.datum_geschat is None:
        _stel_schatting_in(d)

    if not d.geschiedenis:
        d._log_versie("fase1_geen_info", "Geen betrouwbare datum gevonden.")

    return d


def _zoek_datum_in_bestandsnaam(pad: str, d: DatumOnzekerheid):
    """Extraheert datum uit bestandsnaam of mappenpad.

    Herkent patronen als:
      - 2014-01-05 15.21.16.jpg   (spatie-gescheiden tijd)
      - 2020-04-19/_MG_5408.JPG   (datum in mapnaam)
      - 20130403_143642.jpg        (compacte camera-naamgeving)
      - IMG_20130403_143642.jpg    (Android-stijl)
    """
    # Patroon 1: YYYY-MM-DD (met optionele tijd erna)
    m = re.search(r"(\d{4})-(\d{2})-(\d{2})", pad)
    if m:
        jaar, maand, dag = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if JAAR_MIN <= jaar <= JAAR_MAX and 1 <= maand <= 12 and 1 <= dag <= 31:
            try:
                date(jaar, maand, dag)  # valideer kalender
            except ValueError:
                pass
            else:
                d.dag, d.maand = dag, maand
                d.jaar_min = d.jaar_max = jaar
                d.zekerheid = 0.85
                d.datum_geschat = f"{jaar}-{maand:02d}-{dag:02d}"
                d.redenering.append({
                    "type": "bestandsnaam_datum",
                    "bewijs": f"Datum in bestandspad: '{m.group()}'",
                    "gewicht": 0.85,
                })
                d._log_versie("bestandsnaam_datum")
                return

    # Patroon 2: YYYYMMDD (compact, bv. 20130403_143642)
    m = re.search(r"(?<!\d)(20\d{2}|19\d{2})(\d{2})(\d{2})(?:[_\s]|\b)", pad)
    if m:
        jaar, maand, dag = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if JAAR_MIN <= jaar <= JAAR_MAX and 1 <= maand <= 12 and 1 <= dag <= 31:
            try:
                date(jaar, maand, dag)
            except ValueError:
                pass
            else:
                d.dag, d.maand = dag, maand
                d.jaar_min = d.jaar_max = jaar
                d.zekerheid = 0.80
                d.datum_geschat = f"{jaar}-{maand:02d}-{dag:02d}"
                d.redenering.append({
                    "type": "bestandsnaam_datum_compact",
                    "bewijs": f"Compacte datum in bestandspad: '{m.group()}'",
                    "gewicht": 0.80,
                })
                d._log_versie("bestandsnaam_datum_compact")
                return


def _parse_exif_datum(s: str) -> Optional[date]:
    for fmt in ("%Y:%m:%d %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s[:19], fmt).date()
        except ValueError:
            continue
    return None


def _zoek_expliciete_datum(tekst: str, d: DatumOnzekerheid):
    """Zoekt expliciete datumvermeldingen in de tekst."""

    # Patroon: "14 maart 1995" / "14 maart '95"
    pat_lang = re.compile(
        r"\b(\d{1,2})\s+(" + "|".join(_MAAND_NL.keys()) + r")"
        r"(?:\s+(?:19|20)?(\d{2,4}))?\b",
        re.IGNORECASE
    )
    for m in pat_lang.finditer(tekst):
        dag_str, maand_str, jaar_str = m.group(1), m.group(2).lower(), m.group(3)
        dag = int(dag_str)
        maand = _MAAND_NL.get(maand_str)
        if not maand:
            continue
        if d.dag is None:
            d.dag, d.maand = dag, maand
        jaar = None
        if jaar_str:
            jaar = int(jaar_str)
            if jaar < 100:
                jaar += 1900 if jaar > 50 else 2000
            d.jaar_min = d.jaar_max = jaar
            d.zekerheid = 0.92
            d.datum_geschat = f"{jaar}-{maand:02d}-{dag:02d}"
            d.redenering.append({
                "type": "expliciete_vermelding",
                "bewijs": f"Gevonden in tekst: '{m.group()}'",
                "gewicht": 0.92,
            })
            d._log_versie("expliciete_datum_volledig")
            return
        else:
            d.zekerheid = max(d.zekerheid, 0.45)
            d.redenering.append({
                "type": "dag_maand_zonder_jaar",
                "bewijs": f"Dag+maand gevonden: '{m.group()}', jaar ontbreekt",
                "gewicht": 0.45,
            })
            break

    # Patroon: "dd/mm/yyyy" of "dd-mm-yyyy"
    pat_num = re.compile(r"\b(\d{1,2})[/\-\.](\d{1,2})[/\-\.](\d{2,4})\b")
    for m in pat_num.finditer(tekst):
        a, b, c = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if c < 100:
            c += 1900 if c > 50 else 2000
        # Heuristiek: als a <= 12 en b <= 12, probeer beide interpretaties
        # maar geef voorkeur aan dag/maand (Belgisch formaat)
        if 1 <= a <= 31 and 1 <= b <= 12:
            d.dag, d.maand = a, b
            d.jaar_min = d.jaar_max = c
            d.zekerheid = 0.88
            d.datum_geschat = f"{c}-{b:02d}-{a:02d}"
            d.redenering.append({
                "type": "numerieke_datum",
                "bewijs": f"dd/mm/yyyy patroon: '{m.group()}'",
                "gewicht": 0.88,
            })
            d._log_versie("numerieke_datum")
            return

    # Dag van de week
    dow_pat = re.compile(r"\b(" + "|".join(_DOW_NL.keys()) + r")\b", re.IGNORECASE)
    m = dow_pat.search(tekst)
    if m:
        d.dag_van_week = m.group(1).lower()


def _kalender_check(d: DatumOnzekerheid):
    """
    Als dag en maand bekend zijn maar jaar niet, bereken kandidaatjaren
    door na te gaan in welke jaren die dag/maand op de bekende weekdag valt.
    """
    import calendar
    kandidaten = []
    for jaar in range(JAAR_MIN, JAAR_MAX + 1):
        try:
            dt = date(jaar, d.maand, d.dag)
        except ValueError:
            continue
        if d.dag_van_week:
            dow_num = _DOW_NL.get(d.dag_van_week)
            if dow_num is not None and dt.weekday() != dow_num:
                continue
        kandidaten.append(jaar)

    if kandidaten:
        d.jaar_kandidaten = kandidaten
        d.jaar_min = kandidaten[0]
        d.jaar_max = kandidaten[-1]
        d.zekerheid = min(d.zekerheid + 0.2, 0.75) if d.dag_van_week else d.zekerheid
        d.redenering.append({
            "type": "kalendercheck",
            "bewijs": f"{d.dag} {d.maand} valt op '{d.dag_van_week}' in: {kandidaten}",
            "gewicht": 0.55 if d.dag_van_week else 0.3,
        })
        d._log_versie("kalendercheck",
                      f"{len(kandidaten)} kandidaatjaren gevonden")


def _stel_schatting_in(d: DatumOnzekerheid):
    if d.jaar_min == d.jaar_max:
        jaar = d.jaar_min
        if d.maand:
            d.datum_geschat = f"{jaar}-{d.maand:02d}" + (f"-{d.dag:02d}" if d.dag else "")
        else:
            d.datum_geschat = str(jaar)
    elif d.jaar_kandidaten:
        midden = d.jaar_kandidaten[len(d.jaar_kandidaten) // 2]
        d.datum_geschat = f"circa {midden}" if d.maand is None else \
                          f"circa {midden}-{d.maand:02d}"
    else:
        span = d.jaar_max - d.jaar_min
        d.datum_geschat = f"{d.jaar_min}–{d.jaar_max}"


# ── Fase 1b: relatieve datering via contextankers ────────────────────────────

def _zoek_absolute_jaren(tekst: str) -> list[int]:
    """Vindt alle expliciete jaartallen (19xx/20xx) in de tekst."""
    return [int(m) for m in re.findall(r"\b((?:19|20)\d{2})\b", tekst)]


def _resolv_relatieve_datums(tekst: str, d: DatumOnzekerheid):
    """
    Als de tekst dag+maand zonder jaar bevat maar elders in dezelfde tekst
    absolute datums staan, gebruik die als anker.

    Voorbeeld: "2 januari" + verderop "10 januari 2015" → 2 januari 2015
    """
    if d.jaar_min == d.jaar_max and d.zekerheid >= 0.8:
        return  # al opgelost

    ankerjaren = _zoek_absolute_jaren(tekst)
    if not ankerjaren:
        return

    # Tel frequenties — het meest genoemde jaar is het sterkste anker
    from collections import Counter
    freq = Counter(ankerjaren)
    meest_voorkomend, n = freq.most_common(1)[0]

    # Alleen toepassen als er een duidelijk ankerjaar is
    if n < 1:
        return

    # Als dag+maand al gevonden maar jaar niet: gebruik anker
    if d.dag and d.maand and d.jaar_min != d.jaar_max:
        d.jaar_min = d.jaar_max = meest_voorkomend
        d.zekerheid = max(d.zekerheid, 0.70)
        d.datum_geschat = f"{meest_voorkomend}-{d.maand:02d}-{d.dag:02d}"
        d.redenering.append({
            "type": "contextanker",
            "bewijs": f"Relatieve datum {d.dag}/{d.maand} opgelost via "
                      f"ankerjaar {meest_voorkomend} ({n}x genoemd in tekst)",
            "gewicht": 0.70,
        })
        d._log_versie("contextanker_relatief",
                      f"Jaar {meest_voorkomend} als anker (freq={n})")
        return

    # Geen dag/maand maar we kunnen het bereik beperken
    if d.jaar_min < min(ankerjaren) or d.jaar_max > max(ankerjaren):
        nieuw_min = max(d.jaar_min, min(ankerjaren) - 1)
        nieuw_max = min(d.jaar_max, max(ankerjaren) + 1)
        if nieuw_min <= nieuw_max:
            d.jaar_min = nieuw_min
            d.jaar_max = nieuw_max
            d.zekerheid = max(d.zekerheid, 0.45)
            d.redenering.append({
                "type": "contextanker_bereik",
                "bewijs": f"Ankerjaren in tekst: {sorted(set(ankerjaren))} → "
                          f"bereik {nieuw_min}–{nieuw_max}",
                "gewicht": 0.45,
            })
            d._log_versie("contextanker_bereik")


# ── Fase 2: globale constraints ───────────────────────────────────────────────

class GlobaleDateringsmotor:
    """
    Past constraints toe over de hele corpus.
    Gebruik na Fase 1, zodra alle documenten een DatumOnzekerheid hebben.
    """

    def __init__(self, actors: dict):
        """
        actors: dict van actor_id → actor-dict (met 'eerste_vermelding',
                'laatste_vermelding', 'overlijden', etc.)
        """
        self.actors = actors

    def pas_actor_constraints_toe(self, doc_id: str, doc_actors: list,
                                   d: DatumOnzekerheid) -> DatumOnzekerheid:
        """
        Beperkt jaarbereik op basis van levensdata van vermelde actoren.
        doc_actors: lijst van {"ref": "actor_001", "rol": "vermeld"}
        """
        for actor_ref in doc_actors:
            ref = actor_ref.get("ref")
            actor = self.actors.get(ref)
            if not actor:
                continue

            overlijden = actor.get("overlijden")
            geboorte   = actor.get("geboorte")

            if overlijden:
                try:
                    jaar_overl = int(overlijden[:4])
                    if d.jaar_max > jaar_overl:
                        d.jaar_max = jaar_overl
                        d.redenering.append({
                            "type": "actorconstraint_overlijden",
                            "bewijs": f"{actor.get('naam')} overleed in {jaar_overl}; "
                                      f"document moet van vóór {jaar_overl} zijn",
                            "bewijs_bron": ref,
                            "gewicht": 0.95,
                        })
                        d.zekerheid = min(d.zekerheid + 0.15, 0.95)
                except (ValueError, TypeError):
                    pass

            if geboorte:
                try:
                    jaar_geb = int(geboorte[:4])
                    if d.jaar_min < jaar_geb:
                        d.jaar_min = jaar_geb
                        d.redenering.append({
                            "type": "actorconstraint_geboorte",
                            "bewijs": f"{actor.get('naam')} geboren in {jaar_geb}; "
                                      f"document kan niet van eerder zijn",
                            "bewijs_bron": ref,
                            "gewicht": 0.9,
                        })
                        d.zekerheid = min(d.zekerheid + 0.1, 0.95)
                except (ValueError, TypeError):
                    pass

        _stel_schatting_in(d)
        d._log_versie("actorconstraints_fase2")
        return d

    def pas_reeks_toe(self, doc_id: str, buren: list,
                      d: DatumOnzekerheid) -> DatumOnzekerheid:
        """
        buren: lijst van DatumOnzekerheid van aangrenzende documenten.
        Gebruikt de mediane schatting als prior.
        """
        jaren = []
        for b in buren:
            if b.jaar_min == b.jaar_max and b.zekerheid > 0.6:
                jaren.append(b.jaar_min)

        if not jaren:
            return d

        import statistics
        mediaan = int(statistics.median(jaren))
        venster = 3  # ± 3 jaar

        nieuw_min = max(d.jaar_min, mediaan - venster)
        nieuw_max = min(d.jaar_max, mediaan + venster)
        if nieuw_min <= nieuw_max:
            d.jaar_min = nieuw_min
            d.jaar_max = nieuw_max
            d.redenering.append({
                "type": "documentreeks",
                "bewijs": f"Omliggende docs clusteren rond {mediaan} "
                          f"(n={len(jaren)}); venster ±{venster} jaar",
                "gewicht": 0.5,
            })
            d.zekerheid = min(d.zekerheid + 0.1, 0.85)
            _stel_schatting_in(d)
            d._log_versie("reeks_prior_fase2",
                          f"Mediaan buren = {mediaan}")
        return d
