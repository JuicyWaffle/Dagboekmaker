"""
dagboekmaker.corpus
~~~~~~~~~~~~~~~~~~~
Beheert het corpus: JSON-bestanden per document + SQLite-database
voor efficiënt opzoeken, filteren en tijdlijnopbouw.

Schema:
  documenten    – één rij per verwerkt document
  actors        – unieke personen/organisaties
  doc_actors    – n:m koppeling document ↔ actor
  levensperiodes – configureerbare dramatische acts
"""

import json
import logging
import sqlite3
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Iterator, Optional

log = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS documenten (
    id              TEXT PRIMARY KEY,
    pad_origineel   TEXT,
    formaat         TEXT,
    datum_geschat   TEXT,
    datum_vroegst   TEXT,
    datum_laatst    TEXT,
    precisie        TEXT,
    zekerheid       REAL,
    type            TEXT,
    taal            TEXT,
    samenvatting    TEXT,
    themas          TEXT,          -- JSON array
    emotionele_toon TEXT,
    spanning        REAL,
    keerpunt        INTEGER,
    levensperiode   TEXT,
    pad_json        TEXT,
    verwerkt_op     TEXT
);

CREATE TABLE IF NOT EXISTS actors (
    id              TEXT PRIMARY KEY,
    naam            TEXT,
    aliassen        TEXT,          -- JSON array
    type            TEXT,
    relatie         TEXT,
    geboorte        TEXT,
    overlijden      TEXT,
    rol_in_verhaal  TEXT,
    narratieve_arc  TEXT
);

CREATE TABLE IF NOT EXISTS doc_actors (
    doc_id          TEXT,
    actor_id        TEXT,
    rol             TEXT,
    PRIMARY KEY (doc_id, actor_id, rol),
    FOREIGN KEY (doc_id)   REFERENCES documenten(id),
    FOREIGN KEY (actor_id) REFERENCES actors(id)
);

CREATE TABLE IF NOT EXISTS levensperiodes (
    id      TEXT PRIMARY KEY,
    label   TEXT,
    jaar_van TEXT,
    jaar_tot TEXT,
    volgorde INTEGER
);

CREATE INDEX IF NOT EXISTS idx_doc_datum  ON documenten(datum_vroegst, datum_laatst);
CREATE INDEX IF NOT EXISTS idx_doc_type   ON documenten(type);
CREATE INDEX IF NOT EXISTS idx_doc_toon   ON documenten(emotionele_toon);
CREATE INDEX IF NOT EXISTS idx_doc_periode ON documenten(levensperiode);
"""

STANDAARD_LEVENSPERIODES = [
    ("kindertijd",     "Kindertijd",         None,   "1985", 1),
    ("adolescentie",   "Adolescentie",       "1985", "1993", 2),
    ("jong_volwassen", "Jong volwassen",     "1993", "2002", 3),
    ("breekpunt",      "Breekpunt",          "2002", "2005", 4),
    ("opbouw",         "Opbouw",             "2005", "2015", 5),
    ("heden",          "Heden",              "2015", None,   6),
]


class Corpus:
    def __init__(self, root: str):
        self.root = Path(root)
        self.json_dir = self.root / "corpus"
        self.json_dir.mkdir(parents=True, exist_ok=True)
        self.db_pad = self.root / "dagboekmaker.db"
        self._db: Optional[sqlite3.Connection] = None

    @property
    def db(self) -> sqlite3.Connection:
        if self._db is None:
            self._db = sqlite3.connect(str(self.db_pad))
            self._db.row_factory = sqlite3.Row
            self._db.executescript(SCHEMA)
            self._migreer_schema()
            self._initialiseer_periodes()
            self._db.commit()
        return self._db

    def sluit(self):
        if self._db:
            self._db.close()
            self._db = None

    # ── Documenten ───────────────────────────────────────────────────────────

    def sla_document_op(self, doc: dict) -> str:
        """
        Slaat een volledig document-dict op als JSON én in de database.
        Geeft het doc-id terug.
        """
        doc_id = doc["id"]
        jaar_subdir = _jaar_uit_schatting(doc.get("tijdstip", {}).get("datum_geschat"))
        subdir = self.json_dir / jaar_subdir
        subdir.mkdir(parents=True, exist_ok=True)

        json_pad = subdir / f"{doc_id}.json"
        json_pad.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")

        doc["pad_json"] = str(json_pad)
        self._upsert_document(doc)
        self._upsert_actors(doc)
        self.db.commit()
        return doc_id

    def haal_document_op(self, doc_id: str) -> Optional[dict]:
        rij = self.db.execute(
            "SELECT pad_json FROM documenten WHERE id = ?", (doc_id,)
        ).fetchone()
        if not rij or not rij["pad_json"]:
            return None
        pad = Path(rij["pad_json"])
        if pad.exists():
            return json.loads(pad.read_text(encoding="utf-8"))
        return None

    def verwijder_document(self, doc_id: str):
        """Verwijdert een document uit JSON en database."""
        rij = self.db.execute(
            "SELECT pad_json FROM documenten WHERE id = ?", (doc_id,)
        ).fetchone()
        if rij and rij["pad_json"]:
            pad = Path(rij["pad_json"])
            if pad.exists():
                pad.unlink()
        self.db.execute("DELETE FROM doc_actors WHERE doc_id = ?", (doc_id,))
        self.db.execute("DELETE FROM documenten WHERE id = ?", (doc_id,))
        self.db.commit()

    def haal_serie_op(self, bron_id: str) -> list[dict]:
        """Haalt alle fragmenten van een bronbestand op, gesorteerd op volgnummer."""
        rijen = self.db.execute(
            "SELECT id FROM documenten WHERE serie_bron_id = ? ORDER BY serie_volgnummer",
            (bron_id,)
        ).fetchall()
        return [self.haal_document_op(r["id"]) for r in rijen]

    def zoek(self, type: str = None, levensperiode: str = None,
             actor_id: str = None, zekerheid_min: float = 0.0,
             keerpunt: bool = None) -> list[dict]:
        """Flexibele zoekopdracht. Geeft lijst van document-dicts terug."""
        where, params = ["1=1"], []
        if type:
            where.append("type = ?"); params.append(type)
        if levensperiode:
            where.append("levensperiode = ?"); params.append(levensperiode)
        if zekerheid_min:
            where.append("zekerheid >= ?"); params.append(zekerheid_min)
        if keerpunt is not None:
            where.append("keerpunt = ?"); params.append(1 if keerpunt else 0)

        sql = f"SELECT id FROM documenten WHERE {' AND '.join(where)} ORDER BY datum_vroegst"
        rijen = self.db.execute(sql, params).fetchall()

        if actor_id:
            actor_ids = {r["doc_id"] for r in
                         self.db.execute("SELECT doc_id FROM doc_actors WHERE actor_id = ?",
                                         (actor_id,)).fetchall()}
            rijen = [r for r in rijen if r["id"] in actor_ids]

        return [self.haal_document_op(r["id"]) for r in rijen]

    def tijdlijn_dichtheid(self) -> dict:
        """
        Geeft per jaar een dict {zeker, onzeker, raw} terug.
        Bruikbaar als invoer voor het dashboard.
        """
        rijen = self.db.execute("""
            SELECT
                SUBSTR(datum_vroegst, 1, 4) as jaar,
                COUNT(*) as totaal,
                SUM(CASE WHEN zekerheid >= 0.8 AND datum_vroegst = datum_laatst THEN 1 ELSE 0 END) as zeker,
                SUM(CASE WHEN zekerheid < 0.8 OR datum_vroegst != datum_laatst THEN 1 ELSE 0 END) as onzeker
            FROM documenten
            WHERE jaar IS NOT NULL AND jaar != ''
            GROUP BY jaar
            ORDER BY jaar
        """).fetchall()
        return {r["jaar"]: {"zeker": r["zeker"], "onzeker": r["onzeker"], "raw": 0}
                for r in rijen}

    def voortgang_stats(self) -> dict:
        """Statistieken voor de voortgangskaarten in het dashboard."""
        rijen = self.db.execute("""
            SELECT type, COUNT(*) as verwerkt FROM documenten GROUP BY type
        """).fetchall()
        return {r["type"]: r["verwerkt"] for r in rijen}

    # ── Actors ───────────────────────────────────────────────────────────────

    def sla_actor_op(self, actor: dict):
        self.db.execute("""
            INSERT OR REPLACE INTO actors
            (id, naam, aliassen, type, relatie, geboorte, overlijden,
             rol_in_verhaal, narratieve_arc)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            actor["id"], actor.get("naam"), json.dumps(actor.get("aliassen", [])),
            actor.get("type", "persoon"), actor.get("relatie_tot_auteur"),
            actor.get("geboorte"), actor.get("overlijden"),
            actor.get("rol_in_verhaal"), actor.get("narratieve_arc"),
        ))
        self.db.commit()

    def haal_alle_actors_op(self) -> dict:
        rijen = self.db.execute("SELECT * FROM actors").fetchall()
        result = {}
        for r in rijen:
            d = dict(r)
            d["aliassen"] = json.loads(d["aliassen"] or "[]")
            result[d["id"]] = d
        return result

    def exporteer_actors_json(self) -> Path:
        pad = self.root / "actors.json"
        pad.write_text(json.dumps(self.haal_alle_actors_op(),
                                  ensure_ascii=False, indent=2), encoding="utf-8")
        return pad

    # ── Interne helpers ──────────────────────────────────────────────────────

    def _upsert_document(self, doc: dict):
        t = doc.get("tijdstip", {})
        n = doc.get("narratief", {})
        s = doc.get("serie", {})
        self.db.execute("""
            INSERT OR REPLACE INTO documenten
            (id, pad_origineel, formaat, datum_geschat, datum_vroegst, datum_laatst,
             precisie, zekerheid, type, taal, samenvatting, themas, emotionele_toon,
             spanning, keerpunt, levensperiode, pad_json, verwerkt_op,
             serie_bron_id, serie_volgnummer, serie_totaal,
             serie_vorige_id, serie_volgende_id)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            doc["id"],
            doc.get("bestand_origineel"),
            doc.get("formaat_origineel"),
            t.get("datum_geschat"),
            t.get("datum_vroegst"),
            t.get("datum_laatst"),
            t.get("precisie"),
            t.get("zekerheid", 0.0),
            doc.get("inhoud", {}).get("type", "onbekend"),
            doc.get("inhoud", {}).get("taal", "nl"),
            doc.get("inhoud", {}).get("samenvatting"),
            json.dumps(doc.get("inhoud", {}).get("themas", [])),
            doc.get("inhoud", {}).get("emotionele_toon"),
            n.get("spanning", 0.0),
            1 if n.get("keerpunt") else 0,
            doc.get("levensperiode"),
            doc.get("pad_json"),
            datetime.utcnow().isoformat(),
            s.get("bron_id"),
            s.get("volgnummer"),
            s.get("totaal"),
            s.get("vorige_id"),
            s.get("volgende_id"),
        ))

    def _upsert_actors(self, doc: dict):
        for actor_ref in doc.get("actors", []):
            ref = actor_ref.get("ref")
            rol = actor_ref.get("rol", "vermeld")
            if not ref:
                continue
            # rol kan een lijst zijn (meerdere rollen per actor)
            rollen = rol if isinstance(rol, list) else [rol]
            for r in rollen:
                try:
                    self.db.execute(
                        "INSERT OR IGNORE INTO doc_actors (doc_id, actor_id, rol) VALUES (?,?,?)",
                        (doc["id"], ref, r)
                    )
                except sqlite3.IntegrityError:
                    pass

    def _migreer_schema(self):
        """Voegt serie-kolommen toe als ze nog niet bestaan (v0.2 migratie)."""
        bestaande = {r[1] for r in self.db.execute("PRAGMA table_info(documenten)").fetchall()}
        nieuw = {
            "serie_bron_id": "TEXT",
            "serie_volgnummer": "INTEGER",
            "serie_totaal": "INTEGER",
            "serie_vorige_id": "TEXT",
            "serie_volgende_id": "TEXT",
        }
        for kolom, type_ in nieuw.items():
            if kolom not in bestaande:
                self.db.execute(f"ALTER TABLE documenten ADD COLUMN {kolom} {type_}")
        self.db.execute(
            "CREATE INDEX IF NOT EXISTS idx_doc_serie "
            "ON documenten(serie_bron_id, serie_volgnummer)"
        )

    def _initialiseer_periodes(self):
        bestaand = self.db.execute("SELECT COUNT(*) FROM levensperiodes").fetchone()[0]
        if bestaand == 0:
            self.db.executemany(
                "INSERT INTO levensperiodes (id,label,jaar_van,jaar_tot,volgorde) VALUES (?,?,?,?,?)",
                STANDAARD_LEVENSPERIODES
            )


def _jaar_uit_schatting(s: Optional[str]) -> str:
    if not s:
        return "ongedateerd"
    import re
    m = re.search(r"\d{4}", s)
    return m.group() if m else "ongedateerd"
