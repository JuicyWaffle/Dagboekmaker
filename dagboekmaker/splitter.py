"""
dagboekmaker.splitter
~~~~~~~~~~~~~~~~~~~~~
Splitst dagboektekst op datumkoppen zodat elk gedateerd fragment
apart gedateerd, verrijkt en opgeslagen kan worden.

Gebruik:
    from dagboekmaker.splitter import splits_dagboek

    result = splits_dagboek(tekst)
    for frag in result.fragmenten:
        print(frag.volgnummer, frag.datum_header, len(frag.tekst))
"""

import re
from dataclasses import dataclass
from typing import Optional

from .datering import _MAAND_NL, _DOW_NL

# ── Dataclasses ──────────────────────────────────────────────────────────────

@dataclass
class Fragment:
    """Eén gedateerd dagboekfragment uit een groter bronbestand."""
    volgnummer: int              # 0-based positie in bron
    tekst: str                   # tekst van dit fragment
    datum_header: Optional[str]  # ruwe datumkop, None voor proloog
    positie_start: int           # char offset in originele tekst
    positie_eind: int            # char offset in originele tekst


@dataclass
class SplitResultaat:
    """Resultaat van splits_dagboek()."""
    fragmenten: list[Fragment]
    is_gesplitst: bool           # True als >1 bruikbaar fragment
    methode: str                 # "datum_headers" | "geen_split"


# ── Regex patronen ───────────────────────────────────────────────────────────

_MAANDEN = "|".join(_MAAND_NL.keys())
_DAGEN = "|".join(_DOW_NL.keys())

# Patroon 1: "14 maart 1995", "14 maart '95", "14 maart" (zonder jaar)
# Optioneel voorafgegaan door weekdag: "Dinsdag 14 maart 1995"
_PAT_LANG = re.compile(
    r"^[ \t]*"
    r"(?:(?:" + _DAGEN + r")[\s,.-]*)"          # optionele weekdag
    r"?(\d{1,2})[\s.\-]+("
    + _MAANDEN +
    r")(?:\s+['\u2018\u2019]?(?:19|20)?(\d{2,4}))?"  # optioneel jaar
    r"[ \t]*$",
    re.IGNORECASE | re.MULTILINE,
)

# Patroon 2: "14/03/1995", "14-03-1995", "14.03.1995"
_PAT_NUM = re.compile(
    r"^[ \t]*"
    r"(?:(?:" + _DAGEN + r")[\s,.-]*)?"         # optionele weekdag
    r"(\d{1,2})[/\-.](\d{1,2})[/\-.](\d{2,4})"
    r"[ \t]*$",
    re.IGNORECASE | re.MULTILINE,
)


# ── Hoofdfunctie ─────────────────────────────────────────────────────────────

def splits_dagboek(tekst: str, min_fragment_len: int = 20) -> SplitResultaat:
    """
    Splitst dagboektekst op datumkoppen.

    Herkent datumkoppen die alleen op een eigen regel staan (kort, <80 tekens).
    Tekst vóór de eerste datumkop wordt een proloog-fragment.
    Hele korte fragmenten (<min_fragment_len) worden samengevoegd met het vorige.

    Geeft altijd minstens 1 fragment terug.
    """
    if not tekst or not tekst.strip():
        return SplitResultaat(
            fragmenten=[Fragment(0, tekst or "", None, 0, len(tekst or ""))],
            is_gesplitst=False,
            methode="geen_split",
        )

    # Zoek alle datumkop-posities
    headers = _vind_datum_headers(tekst)

    if len(headers) < 2:
        # 0 of 1 datumkop → geen splitsing
        datum_header = headers[0][1] if headers else None
        return SplitResultaat(
            fragmenten=[Fragment(0, tekst, datum_header, 0, len(tekst))],
            is_gesplitst=False,
            methode="geen_split",
        )

    # Bouw fragmenten op basis van header-posities
    fragmenten = []
    for i, (pos, header_tekst) in enumerate(headers):
        eind = headers[i + 1][0] if i + 1 < len(headers) else len(tekst)
        frag_tekst = tekst[pos:eind].rstrip()
        fragmenten.append(Fragment(
            volgnummer=i,
            tekst=frag_tekst,
            datum_header=header_tekst,
            positie_start=pos,
            positie_eind=eind,
        ))

    # Proloog: tekst vóór eerste header
    eerste_pos = headers[0][0]
    if eerste_pos > 0:
        proloog_tekst = tekst[:eerste_pos].rstrip()
        if proloog_tekst.strip():
            proloog = Fragment(
                volgnummer=0,
                tekst=proloog_tekst,
                datum_header=None,
                positie_start=0,
                positie_eind=eerste_pos,
            )
            # Hernummer bestaande fragmenten
            for f in fragmenten:
                f.volgnummer += 1
            fragmenten.insert(0, proloog)

    # Merge te korte fragmenten met het voorgaande
    samengevoegd = []
    for frag in fragmenten:
        inhoud = frag.tekst
        # Strip de header-regel zelf om alleen de body te meten
        if frag.datum_header:
            body = inhoud[len(frag.datum_header):].strip()
        else:
            body = inhoud.strip()

        if samengevoegd and len(body) < min_fragment_len:
            # Voeg samen met vorig fragment
            vorig = samengevoegd[-1]
            samengevoegd[-1] = Fragment(
                volgnummer=vorig.volgnummer,
                tekst=vorig.tekst + "\n\n" + frag.tekst,
                datum_header=vorig.datum_header,
                positie_start=vorig.positie_start,
                positie_eind=frag.positie_eind,
            )
        else:
            samengevoegd.append(frag)

    # Hernummer definitief
    for i, f in enumerate(samengevoegd):
        f.volgnummer = i

    is_gesplitst = len(samengevoegd) > 1
    return SplitResultaat(
        fragmenten=samengevoegd,
        is_gesplitst=is_gesplitst,
        methode="datum_headers" if is_gesplitst else "geen_split",
    )


# ── Helpers ──────────────────────────────────────────────────────────────────

def _vind_datum_headers(tekst: str) -> list[tuple[int, str]]:
    """
    Vindt datumkoppen in de tekst.

    Retourneert lijst van (positie, header_tekst) gesorteerd op positie.
    Alleen regels die kort genoeg zijn (<80 tekens) en waar de datum
    het dominante element is, tellen als header.
    """
    gevonden = {}  # positie → header_tekst (dedup op positie)

    for pat in (_PAT_LANG, _PAT_NUM):
        for m in pat.finditer(tekst):
            # Check: de hele regel moet kort zijn
            regel_start = tekst.rfind("\n", 0, m.start()) + 1
            regel_eind = tekst.find("\n", m.end())
            if regel_eind == -1:
                regel_eind = len(tekst)
            regel = tekst[regel_start:regel_eind]

            if len(regel.strip()) > 80:
                continue  # datum zit in een lange zin, geen header

            # Gebruik regel_start als positie (begin van de regel)
            gevonden[regel_start] = m.group().strip()

    return sorted(gevonden.items(), key=lambda x: x[0])
