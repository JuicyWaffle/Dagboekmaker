#!/usr/bin/env python3
"""
Dagboekmaker admin server — beheerinterface voor de pipeline.
Draait op port 8095, stdlib only.
"""

import http.server
import json
import logging
import os
import sys
import threading
import time
import traceback
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# Zorg dat het dagboekmaker package importeerbaar is
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(name)s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("admin")

PORT = 8095
CONFIG_PAD = Path.home() / ".dagboekmaker" / "config.json"
ADMIN_DIR = Path(__file__).resolve().parent
PROJECT_DIR = ADMIN_DIR.parent

STANDAARD_CONFIG = {
    "corpus_pad": str(Path.home() / "projects" / "Dagboekmaker" / "corpus"),
    "bronnen": [],
    "backend": "ollama",
    "tweede_pass": True,
}


# ── Config ────────────────────────────────────────────────────────────────────

def laad_config() -> dict:
    CONFIG_PAD.parent.mkdir(parents=True, exist_ok=True)
    if CONFIG_PAD.exists():
        return json.loads(CONFIG_PAD.read_text(encoding="utf-8"))
    return dict(STANDAARD_CONFIG)


def sla_config_op(cfg: dict):
    CONFIG_PAD.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PAD.write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")


# ── PipelineWorker ────────────────────────────────────────────────────────────

class PipelineWorker:
    """Beheert de pipeline in een achtergrondthread."""

    def __init__(self):
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._pause_event = threading.Event()
        self._pause_event.set()  # niet gepauzeerd bij start
        self._lock = threading.Lock()

        self.status = "idle"  # idle / running / paused / stopping / klaar / fout
        self.voortgang = {}
        self.fouten = []
        self.start_tijd = None

    def start(self):
        with self._lock:
            if self.status in ("running", "paused", "stopping"):
                return False
            self._stop_event.clear()
            self._pause_event.set()
            self.status = "running"
            self.voortgang = {}
            self.fouten = []
            self.start_tijd = time.time()
            self._thread = threading.Thread(target=self._run, daemon=True)
            self._thread.start()
            return True

    def pause(self):
        with self._lock:
            if self.status != "running":
                return False
            self._pause_event.clear()
            self.status = "paused"
            return True

    def resume(self):
        with self._lock:
            if self.status != "paused":
                return False
            self._pause_event.set()
            self.status = "running"
            return True

    def stop(self):
        with self._lock:
            if self.status not in ("running", "paused"):
                return False
            self._stop_event.set()
            self._pause_event.set()  # ontgrendel eventuele pauze
            self.status = "stopping"
            return True

    def _voortgang_callback(self, info: dict):
        self.voortgang = info
        if info.get("fase") == "fout":
            self.fouten.append({
                "bestand": info.get("bestand", "?"),
                "fout": info.get("fout", "onbekend"),
            })

    def _run(self):
        try:
            # Claim maximale resources voor de pipeline-thread
            import os
            try:
                os.nice(-10)  # hoge prioriteit (vereist root, anders beste poging)
            except OSError:
                try:
                    os.nice(-5)
                except OSError:
                    pass  # geen rechten, draait op normale prioriteit

            from dagboekmaker.pipeline import Pipeline

            cfg = laad_config()
            corpus_pad = cfg.get("corpus_pad", STANDAARD_CONFIG["corpus_pad"])
            bronnen = cfg.get("bronnen", [])
            backend = cfg.get("backend", "ollama")
            tweede_pass = cfg.get("tweede_pass", True)

            if not bronnen:
                self.status = "fout"
                self.voortgang = {"fase": "fout", "fout": "Geen bronpaden geconfigureerd"}
                return

            for bron in bronnen:
                if self._stop_event.is_set():
                    break

                bron_pad = Path(bron).expanduser()
                if not bron_pad.exists():
                    self.fouten.append({"bestand": str(bron), "fout": "Pad bestaat niet"})
                    continue

                log.info("Verwerk bron: %s", bron_pad)
                self.voortgang = {"fase": "start", "bron": str(bron_pad)}

                p = Pipeline(
                    bronmap=str(bron_pad),
                    corpusmap=corpus_pad,
                    backend=backend,
                    tweede_pass=tweede_pass,
                )
                p.verwerk_alles(
                    voortgang_callback=self._voortgang_callback,
                    stop_event=self._stop_event,
                    pause_event=self._pause_event,
                )

            if self._stop_event.is_set():
                self.status = "idle"
                self.voortgang["fase"] = "gestopt"
            else:
                self.status = "klaar"
                self.voortgang["fase"] = "klaar"

        except Exception as e:
            log.error("Pipeline fout: %s", traceback.format_exc())
            self.status = "fout"
            self.voortgang = {"fase": "fout", "fout": str(e)}

    def als_dict(self) -> dict:
        duur = None
        if self.start_tijd:
            duur = round(time.time() - self.start_tijd)
        return {
            "status": self.status,
            "voortgang": self.voortgang,
            "fouten": self.fouten[-20:],
            "duur_seconden": duur,
        }


# ── HTTP Handler ──────────────────────────────────────────────────────────────

worker = PipelineWorker()


class DagboekHandler(http.server.BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        log.info(format, *args)

    def _json_response(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length))

    # ── GET routes ────────────────────────────────────────────────────────

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path == "/":
            self._serve_file(ADMIN_DIR / "index.html", "text/html")
        elif path == "/dashboard":
            self._serve_file(PROJECT_DIR / "dashboard" / "index.html", "text/html")
        elif path == "/dashboard_data.json":
            self._handle_dashboard_data()
        elif path == "/api/config":
            self._json_response(laad_config())
        elif path == "/api/browse":
            self._handle_browse(qs)
        elif path == "/api/pipeline/status":
            self._json_response(worker.als_dict())
        elif path == "/api/stats":
            self._handle_stats()
        elif path == "/api/actors":
            self._handle_actors()
        elif path == "/actoren":
            self._serve_file(ADMIN_DIR / "actoren.html", "text/html")
        elif path == "/formaten":
            self._serve_file(ADMIN_DIR / "formaten.html", "text/html")
        elif path == "/api/formaten":
            self._handle_formaten()
        else:
            self.send_error(404)

    def _serve_file(self, pad: Path, content_type: str):
        if not pad.exists():
            self.send_error(404)
            return
        data = pad.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", f"{content_type}; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _handle_browse(self, qs):
        pad = qs.get("path", ["/"])[0]
        pad = Path(pad).expanduser()
        if not pad.exists() or not pad.is_dir():
            self._json_response({"error": "Pad bestaat niet", "pad": str(pad)}, 400)
            return

        items = []
        try:
            for entry in sorted(pad.iterdir()):
                if entry.name.startswith("."):
                    continue
                items.append({
                    "naam": entry.name,
                    "pad": str(entry),
                    "is_dir": entry.is_dir(),
                })
        except PermissionError:
            pass

        self._json_response({
            "huidig": str(pad),
            "parent": str(pad.parent) if pad != pad.parent else None,
            "items": items,
        })

    def _handle_stats(self):
        try:
            from dagboekmaker.corpus import Corpus
            cfg = laad_config()
            c = Corpus(cfg.get("corpus_pad", STANDAARD_CONFIG["corpus_pad"]))
            stats = c.voortgang_stats()
            dichtheid = c.tijdlijn_dichtheid()
            totaal = sum(stats.values()) if stats else 0
            c.sluit()
            self._json_response({
                "totaal": totaal,
                "per_type": stats,
                "dichtheid": dichtheid,
            })
        except Exception as e:
            self._json_response({"totaal": 0, "per_type": {}, "dichtheid": {}, "fout": str(e)})

    def _handle_dashboard_data(self):
        """Live dashboard data uit de DB, compatibel met dashboard_data.json formaat."""
        try:
            from dagboekmaker.corpus import Corpus
            cfg = laad_config()
            c = Corpus(cfg.get("corpus_pad", STANDAARD_CONFIG["corpus_pad"]))
            voortgang = c.voortgang_stats()
            dichtheid = c.tijdlijn_dichtheid()
            c.sluit()
            self._json_response({
                "voortgang": voortgang,
                "dichtheid": dichtheid,
                "gegenereerd_op": datetime.now().astimezone().isoformat(),
            })
        except Exception as e:
            self._json_response({"voortgang": {}, "dichtheid": {}, "fout": str(e)})

    def _handle_formaten(self):
        """Retourneer alle ondersteunde formaten en documenttypes."""
        from dagboekmaker.extractor import EXTENSIONS
        from dagboekmaker.verrijking import DOCUMENT_TYPES

        # Groepeer extensies per methode
        per_methode = {}
        for ext, methode in sorted(EXTENSIONS.items()):
            per_methode.setdefault(methode, []).append(ext)

        # Categoriseer
        categorieen = {
            "plaintext": "Platte tekst",
            "libreoffice": "Tekstverwerkers & Office",
            "libwpd": "WordPerfect",
            "pdfminer": "PDF",
            "html": "Web / HTML",
            "email": "E-mail",
            "msg": "Outlook",
            "mbox": "Mailbox",
            "epub": "E-book (EPUB)",
            "mobi": "E-book (MOBI)",
            "djvu": "DjVu",
            "ps": "PostScript",
            "xps": "XPS",
            "chm": "Compiled HTML Help",
            "image": "Afbeeldingen (EXIF/OCR)",
        }

        formaten = []
        for methode, extensies in per_methode.items():
            formaten.append({
                "categorie": categorieen.get(methode, methode),
                "methode": methode,
                "extensies": sorted(extensies),
            })

        self._json_response({
            "formaten": formaten,
            "document_types": DOCUMENT_TYPES,
            "totaal_extensies": len(EXTENSIONS),
        })

    def _handle_actors(self):
        """Retourneer alle actoren met documenttelling, gesorteerd op frequentie."""
        try:
            from dagboekmaker.corpus import Corpus
            cfg = laad_config()
            c = Corpus(cfg.get("corpus_pad", STANDAARD_CONFIG["corpus_pad"]))

            # Haal actoren uit DB
            db_actors = c.haal_alle_actors_op()

            # Tel documenten per actor via doc_actors tabel
            tellingen = {}
            try:
                rijen = c.db.execute(
                    "SELECT actor_id, COUNT(*) as n FROM doc_actors GROUP BY actor_id"
                ).fetchall()
                tellingen = {r["actor_id"]: r["n"] for r in rijen}
            except Exception:
                pass

            # Haal ook de vaste actoren uit actors.json
            actors_json = PROJECT_DIR / "actors" / "actors.json"
            vaste = {}
            if actors_json.exists():
                vaste = json.loads(actors_json.read_text(encoding="utf-8"))

            # Merge: DB actoren + vaste actoren
            alle = {}
            for aid, a in vaste.items():
                alle[aid] = {
                    "id": aid,
                    "naam": a.get("naam", "?"),
                    "aliassen": a.get("aliassen", []),
                    "type": a.get("type", "persoon"),
                    "relatie": a.get("relatie_tot_auteur", ""),
                    "rol": a.get("rol_in_verhaal", ""),
                    "geboorte": a.get("geboorte"),
                    "overlijden": a.get("overlijden"),
                    "documenten": tellingen.get(aid, a.get("documenten_count", 0)),
                }
            for aid, a in db_actors.items():
                if aid not in alle:
                    alle[aid] = {
                        "id": aid,
                        "naam": a.get("naam", "?"),
                        "aliassen": a.get("aliassen", []),
                        "type": a.get("type", "persoon"),
                        "relatie": a.get("relatie", ""),
                        "rol": a.get("rol_in_verhaal", ""),
                        "geboorte": a.get("geboorte"),
                        "overlijden": a.get("overlijden"),
                        "documenten": tellingen.get(aid, 0),
                    }

            # Sorteer op frequentie (aflopend)
            gesorteerd = sorted(alle.values(), key=lambda a: a["documenten"], reverse=True)
            c.sluit()
            self._json_response(gesorteerd)
        except Exception as e:
            self._json_response({"error": str(e)}, 500)

    # ── POST routes ───────────────────────────────────────────────────────

    def do_POST(self):
        path = urlparse(self.path).path

        if path == "/api/config":
            cfg = self._read_body()
            sla_config_op(cfg)
            self._json_response({"ok": True})
        elif path == "/api/pipeline/start":
            ok = worker.start()
            self._json_response({"ok": ok, "status": worker.status})
        elif path == "/api/pipeline/pause":
            ok = worker.pause()
            self._json_response({"ok": ok, "status": worker.status})
        elif path == "/api/pipeline/resume":
            ok = worker.resume()
            self._json_response({"ok": ok, "status": worker.status})
        elif path == "/api/pipeline/stop":
            ok = worker.stop()
            self._json_response({"ok": ok, "status": worker.status})
        elif path == "/api/pipeline/test":
            self._handle_test()
        elif path == "/api/actors/merge":
            self._handle_actors_merge()
        else:
            self.send_error(404)

    def _handle_test(self):
        """Verwerkt 1 bestand dry-run en geeft het resultaat terug."""
        import traceback
        try:
            from dagboekmaker.extractor import extraheer, EXTENSIONS
            from dagboekmaker.datering import dateer_lokaal
            from dagboekmaker.verrijking import maak_verrijker

            cfg = laad_config()
            bronnen = cfg.get("bronnen", [])
            backend = cfg.get("backend", "ollama")

            if not bronnen:
                self._json_response({"error": "Geen bronpaden geconfigureerd"}, 400)
                return

            # Zoek het eerste verwerkbare bestand
            test_bestand = None
            for bron in bronnen:
                bron_pad = Path(bron).expanduser()
                if not bron_pad.exists():
                    continue
                for p in bron_pad.glob("**/*"):
                    if p.is_file() and p.suffix.lower() in EXTENSIONS:
                        test_bestand = p
                        break
                if test_bestand:
                    break

            if not test_bestand:
                self._json_response({"error": "Geen verwerkbare bestanden gevonden"}, 400)
                return

            # Extractie
            extractie = extraheer(str(test_bestand))
            tekst = extractie.tekst or ""

            # Datering
            datum = dateer_lokaal(
                tekst,
                bestandsdatum=extractie.bestandsdatum,
                exif=extractie.exif or {},
            )

            # Verrijking
            verrijker = maak_verrijker(backend)
            model_naam = getattr(verrijker, 'model', backend)
            verrijking = verrijker.verrijk(tekst)

            self._json_response({
                "bestand": str(test_bestand),
                "bestandsnaam": test_bestand.name,
                "formaat": extractie.formaat,
                "methode": extractie.methode,
                "tekst_preview": tekst[:300],
                "model": model_naam,
                "backend": backend,
                "extractie_fout": extractie.fout,
                "datum": datum.als_dict(),
                "verrijking": {
                    "type": verrijking.type,
                    "samenvatting": verrijking.samenvatting,
                    "taal": verrijking.taal,
                    "themas": verrijking.themas,
                    "emotionele_toon": verrijking.emotionele_toon,
                    "18plus": verrijking.achttienplusinhoud,
                    "actoren": verrijking.actoren,
                    "narratief": verrijking.narratief,
                    "datering_hints": verrijking.datering_hints,
                    "fout": verrijking.fout,
                },
            })
        except Exception as e:
            log.error("Test mislukt: %s", traceback.format_exc())
            self._json_response({"error": str(e)}, 500)

    def _handle_actors_merge(self):
        """Voeg dubbele actoren samen. Verwacht JSON: {behoud_id, verwijder_ids: [...]}"""
        try:
            body = self._read_body()
            behoud_id = body.get("behoud_id")
            verwijder_ids = body.get("verwijder_ids", [])

            if not behoud_id or not verwijder_ids:
                self._json_response({"error": "behoud_id en verwijder_ids vereist"}, 400)
                return

            from dagboekmaker.corpus import Corpus
            cfg = laad_config()
            c = Corpus(cfg.get("corpus_pad", STANDAARD_CONFIG["corpus_pad"]))

            # 1. Update doc_actors: verwijder_ids → behoud_id
            for vid in verwijder_ids:
                # Verplaats koppelingen naar behoud_id (negeer dubbels)
                bestaande = c.db.execute(
                    "SELECT doc_id, rol FROM doc_actors WHERE actor_id = ?", (vid,)
                ).fetchall()
                for r in bestaande:
                    try:
                        c.db.execute(
                            "INSERT OR IGNORE INTO doc_actors (doc_id, actor_id, rol) VALUES (?,?,?)",
                            (r["doc_id"], behoud_id, r["rol"])
                        )
                    except Exception:
                        pass
                # Verwijder oude koppelingen
                c.db.execute("DELETE FROM doc_actors WHERE actor_id = ?", (vid,))

            # 2. Merge actor-info: aliassen uitbreiden
            behoud_actor = c.db.execute(
                "SELECT * FROM actors WHERE id = ?", (behoud_id,)
            ).fetchone()
            if behoud_actor:
                aliassen = json.loads(behoud_actor["aliassen"] or "[]")
                for vid in verwijder_ids:
                    oud = c.db.execute("SELECT * FROM actors WHERE id = ?", (vid,)).fetchone()
                    if oud:
                        oud_naam = oud["naam"] or ""
                        if oud_naam and oud_naam not in aliassen and oud_naam != behoud_actor["naam"]:
                            aliassen.append(oud_naam)
                        oud_aliassen = json.loads(oud["aliassen"] or "[]")
                        for a in oud_aliassen:
                            if a not in aliassen and a != behoud_actor["naam"]:
                                aliassen.append(a)
                c.db.execute(
                    "UPDATE actors SET aliassen = ? WHERE id = ?",
                    (json.dumps(aliassen), behoud_id)
                )

            # 3. Verwijder de oude actoren
            for vid in verwijder_ids:
                c.db.execute("DELETE FROM actors WHERE id = ?", (vid,))

            c.db.commit()

            # 4. Update JSON-bestanden: vervang actor refs
            bijgewerkt = 0
            alle_docs = c.db.execute("SELECT id FROM documenten").fetchall()
            for row in alle_docs:
                doc = c.haal_document_op(row["id"])
                if not doc:
                    continue
                actors = doc.get("actors", [])
                gewijzigd = False
                nieuwe_actors = []
                gezien_behoud = False
                for a in actors:
                    if a.get("ref") in verwijder_ids:
                        if not gezien_behoud:
                            # Vervang door behoud_id, merge rollen
                            behoud_entry = next((x for x in actors if x.get("ref") == behoud_id), None)
                            if not behoud_entry:
                                a["ref"] = behoud_id
                                nieuwe_actors.append(a)
                                gezien_behoud = True
                        gewijzigd = True
                    elif a.get("ref") == behoud_id:
                        nieuwe_actors.append(a)
                        gezien_behoud = True
                    else:
                        nieuwe_actors.append(a)
                if gewijzigd:
                    doc["actors"] = nieuwe_actors
                    json_pad = doc.get("pad_json")
                    if json_pad and Path(json_pad).exists():
                        Path(json_pad).write_text(
                            json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8"
                        )
                    bijgewerkt += 1

            c.sluit()

            # 5. Update actors.json (vaste actoren)
            actors_json = PROJECT_DIR / "actors" / "actors.json"
            if actors_json.exists():
                vaste = json.loads(actors_json.read_text(encoding="utf-8"))
                gewijzigd = False
                for vid in verwijder_ids:
                    if vid in vaste:
                        # Merge aliassen naar behoud
                        if behoud_id in vaste:
                            ba = vaste[behoud_id]
                            va = vaste[vid]
                            aliassen = ba.get("aliassen", [])
                            if va.get("naam") and va["naam"] not in aliassen:
                                aliassen.append(va["naam"])
                            for a in va.get("aliassen", []):
                                if a not in aliassen:
                                    aliassen.append(a)
                            ba["aliassen"] = aliassen
                        del vaste[vid]
                        gewijzigd = True
                if gewijzigd:
                    actors_json.write_text(
                        json.dumps(vaste, ensure_ascii=False, indent=2), encoding="utf-8"
                    )

            self._json_response({
                "ok": True,
                "behoud_id": behoud_id,
                "verwijderd": verwijder_ids,
                "documenten_bijgewerkt": bijgewerkt,
            })
        except Exception as e:
            log.error("Actor merge fout: %s", traceback.format_exc())
            self._json_response({"error": str(e)}, 500)


# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("Dagboekmaker admin op port %d", PORT)
    server = http.server.HTTPServer(("", PORT), DagboekHandler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        log.info("Server gestopt.")
        server.server_close()
