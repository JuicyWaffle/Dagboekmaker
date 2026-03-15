# Dagboekmaker

Persoonlijk archief → verrijkte corpus → scriptbasis.

Dagboekmaker speurt naar leesbare bestanden in alle gangbare en historische
tekstformaten, extraheert de inhoud, dateert elk document zo precies mogelijk
(ook als alleen dag en maand bekend zijn), en slaat alles op in een doorzoekbare
structuur die als basis dient voor een autobiografisch script.

---

## Functionaliteit

| Module | Wat het doet |
|--------|--------------|
| `extractor` | Leest TXT, RTF, DOC/DOCX, WP4–6, ODT, PDF, HTML, EML, JPEG/TIFF |
| `datering` | Twee-fase datering met onzekerheidsscores en redeneerlog |
| `verrijking` | LLM-analyse: type, samenvatting, thema's, toon, actoren, spanning |
| `corpus` | JSON per document + SQLite-database |
| `pipeline` | Orkestrator die alles aan elkaar koppelt |
| `cli` | Command-line interface |
| `dashboard/` | Interactief HTML-dashboard met voortgang + tijdlijn |

---

## Installatie

```bash
# Kloon of pak de zip uit
cd dagboekmaker

# Installeer Python-pakketten
pip install -r requirements.txt

# Systeemtools (optioneel maar aanbevolen)
sudo apt install libreoffice-headless libwpd-tools poppler-utils exiftool
```

### LLM-backend kiezen

**Anthropic (Claude)** — snelst, vereist API-sleutel:
```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```

**Ollama (lokaal)** — gratis, vereist Ollama op `localhost:11434`:
```bash
ollama pull qwen2.5:7b   # of een ander model
```

---

## Gebruik

### Alles verwerken
```bash
python -m dagboekmaker.cli verwerk \
  --bron /pad/naar/archief \
  --corpus /pad/naar/output \
  --backend anthropic        # of: --backend ollama --model qwen2.5:7b
```

### Voortgang bekijken
```bash
python -m dagboekmaker.cli stats --corpus /pad/naar/output
```

### Tijdlijn in terminal
```bash
python -m dagboekmaker.cli tijdlijn --corpus /pad/naar/output
```

### Zoeken
```bash
# Alle brieven
python -m dagboekmaker.cli zoek --corpus /pad --type brief

# Keerpuntmomenten in de breekperiode
python -m dagboekmaker.cli zoek --corpus /pad --periode breekpunt --keerpunten

# Alle docs met een specifieke actor
python -m dagboekmaker.cli zoek --corpus /pad --actor actor_moeder
```

### Narratieve briefing (voor scriptwerk)
```bash
python -m dagboekmaker.cli narratief --corpus /pad --periode jong_volwassen
```

### Dashboard
Open `dashboard/index.html` in een browser. Als `dashboard_data.json` in
dezelfde map staat (wordt automatisch aangemaakt door de pipeline), laadt
het dashboard echte data. Anders toont het voorbeelddata.

---

## Outputstructuur

```
output/
├── corpus/
│   ├── 1993/
│   │   └── doc_abc123.json   ← één bestand per document
│   ├── 1995/
│   └── ongedateerd/
├── actors/
│   └── actors.json           ← centrale actorsdatabase
├── dagboekmaker.db           ← SQLite voor query's
├── dashboard_data.json       ← invoer voor het dashboard
└── dashboard/
    └── index.html
```

### Document JSON-formaat

Elk document bevat:
- `tijdstip` — datum met onzekerheidsbandbreedte, zekerheidscore en redeneerlog
- `inhoud` — plaintext, samenvatting, thema's, emotionele toon
- `actors` — verwijzingen naar de actorsdatabase
- `narratief` — spanning (0–1), keerpunt (true/false), levensperiode
- `verwerkings_meta` — welke tool, welke methode, wanneer

### Datumzekerheid

| Precisie | Omschrijving |
|----------|--------------|
| `dag` | Exacte datum bekend (zekerheid ≥ 0.85) |
| `maand` | Maand+jaar bekend |
| `jaar` | Alleen jaar bekend |
| `circa_2jaar` | Bandbreedte ≤ 2 jaar |
| `circa_5jaar` | Bandbreedte ≤ 5 jaar |
| `onbekend` | Brede onzekerheid |

Documenten met zekerheid < 0.6 komen in de review-wachtrij (zie `stats`).

---

## Levensperiodes aanpassen

In `dagboekmaker/corpus.py`, variabele `STANDAARD_LEVENSPERIODES`:

```python
STANDAARD_LEVENSPERIODES = [
    ("kindertijd",     "Kindertijd",     None,   "1985", 1),
    ("adolescentie",   "Adolescentie",   "1985", "1993", 2),
    ("jong_volwassen", "Jong volwassen", "1993", "2002", 3),
    # ... pas aan naar jouw leven
]
```

---

## Systeemvereisten

- Python 3.11+
- LibreOffice (headless) — voor DOC, DOCX, RTF, ODT, WPS
- libwpd-tools (`wpd2text`) — voor WordPerfect WP4–6
- poppler-utils (`pdftotext`) — fallback voor PDF
- exiftool — voor foto-metadata (datum, GPS, camera)

---

## Licentie

MIT
