#!/usr/bin/env python3
"""
dagboekmaker — command-line interface

Gebruik:
  python -m dagboekmaker.cli verwerk --bron /pad/archief --corpus /pad/output
  python -m dagboekmaker.cli stats   --corpus /pad/output
  python -m dagboekmaker.cli zoek    --corpus /pad/output --type brief
  python -m dagboekmaker.cli tijdlijn --corpus /pad/output
"""

import argparse
import json
import logging
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


def cmd_verwerk(args):
    from dagboekmaker.pipeline import Pipeline
    p = Pipeline(
        bronmap=args.bron,
        corpusmap=args.corpus,
        backend=args.backend,
        verrijker_kwargs={"model": args.model} if args.model else {},
        herverwerk=args.herverwerk,
        split_dagboeken=not args.geen_split,
    )
    p.verwerk_alles(glob=args.glob)


def cmd_stats(args):
    from dagboekmaker.corpus import Corpus
    c = Corpus(args.corpus)
    stats = c.voortgang_stats()
    dichtheid = c.tijdlijn_dichtheid()
    totaal = sum(stats.values())
    print(f"\n{'Type':<16} {'Verwerkt':>10}")
    print("─" * 28)
    for t, n in sorted(stats.items(), key=lambda x: -x[1]):
        print(f"{t:<16} {n:>10}")
    print(f"\nTotaal verwerkt: {totaal}")

    jaren = sorted(dichtheid.keys())
    if jaren:
        print(f"\nTijdspanne: {jaren[0]} – {jaren[-1]}")
        gaten = [j for j in jaren if dichtheid[j]["zeker"] == 0]
        if gaten:
            print(f"Jaren zonder gedateerde docs: {', '.join(gaten[:10])}"
                  + (" ..." if len(gaten) > 10 else ""))
    c.sluit()


def cmd_zoek(args):
    from dagboekmaker.corpus import Corpus
    c = Corpus(args.corpus)
    docs = c.zoek(
        type=args.type,
        levensperiode=args.periode,
        actor_id=args.actor,
        zekerheid_min=args.zekerheid,
        keerpunt=True if args.keerpunten else None,
    )
    print(f"\n{len(docs)} document(en) gevonden:\n")
    for d in docs[:args.max]:
        if not d:
            continue
        t = d.get("tijdstip", {})
        print(f"  {d['id']}  {t.get('datum_geschat','?'):15}  "
              f"[{d.get('type','?'):12}]  {d.get('inhoud',{}).get('samenvatting','')[:60]}")
    c.sluit()


def cmd_tijdlijn(args):
    from dagboekmaker.corpus import Corpus
    c = Corpus(args.corpus)
    dichtheid = c.tijdlijn_dichtheid()
    breedte = 40
    max_total = max((v["zeker"] + v["onzeker"] + v["raw"])
                    for v in dichtheid.values()) if dichtheid else 1
    print(f"\n{'Jaar':<6}  {'Zeker':>6}  {'Onzeker':>7}   Grafiek")
    print("─" * 60)
    for jaar in sorted(dichtheid.keys()):
        v = dichtheid[jaar]
        totaal = v["zeker"] + v["onzeker"] + v["raw"]
        bar_len = int(totaal / max_total * breedte)
        z_len   = int(v["zeker"] / max_total * breedte)
        o_len   = int(v["onzeker"] / max_total * breedte)
        bar = "█" * z_len + "░" * o_len + "·" * max(0, bar_len - z_len - o_len)
        print(f"{jaar:<6}  {v['zeker']:>6}  {v['onzeker']:>7}   {bar}")
    print("\n  █ = gedateerd   ░ = onzeker jaar   · = onverwerkt")
    c.sluit()


def cmd_narratief(args):
    """Genereert een narratieve briefing voor een levensperiode."""
    from dagboekmaker.corpus import Corpus
    c = Corpus(args.corpus)
    docs = c.zoek(levensperiode=args.periode)
    actors = c.haal_alle_actors_op()

    thema_teller: dict = {}
    tonen: list = []
    keerpunten = []

    for d in docs:
        if not d:
            continue
        inhoud = d.get("inhoud", {})
        for t in inhoud.get("themas", []):
            thema_teller[t] = thema_teller.get(t, 0) + 1
        toon = inhoud.get("emotionele_toon")
        if toon:
            tonen.append(toon)
        if d.get("narratief", {}).get("keerpunt"):
            keerpunten.append(d)

    top_themas = sorted(thema_teller, key=lambda k: -thema_teller[k])[:5]

    print(f"\n{'='*60}")
    print(f"NARRATIEVE BRIEFING — {args.periode.upper()}")
    print(f"{'='*60}")
    print(f"Documenten:       {len(docs)}")
    print(f"Top thema's:      {', '.join(top_themas)}")
    print(f"Dominante toon:   {max(set(tonen), key=tonen.count) if tonen else '?'}")
    print(f"Keerpunten:       {len(keerpunten)}")
    if keerpunten:
        print("\nKeerpuntmomenten:")
        for d in keerpunten[:5]:
            t = d.get("tijdstip", {})
            print(f"  {t.get('datum_geschat','?'):12}  "
                  f"{d.get('inhoud',{}).get('samenvatting','')[:70]}")
    c.sluit()


def cmd_gaten(args):
    """Toont tijdlijn-gaten (periodes zonder documenten)."""
    from dagboekmaker.corpus import Corpus
    c = Corpus(args.corpus)
    gaten = c.tijdlijn_gaten(min_gap_maanden=args.min_maanden)

    if not gaten:
        print("\nGeen significante gaten gevonden in de tijdlijn.")
        c.sluit()
        return

    print(f"\n{'Van':<12} {'Tot':<12} {'Duur':>6}   Opmerking")
    print("─" * 55)
    for g in gaten:
        duur = f"{g['duur_maanden']}m"
        bar = "░" * min(g["duur_maanden"], 30)
        print(f"{g['van']:<12} {g['tot']:<12} {duur:>6}   {bar}")

    totaal_maanden = sum(g["duur_maanden"] for g in gaten)
    print(f"\n{len(gaten)} gaten, totaal ~{totaal_maanden} maanden ongedocumenteerd")
    print("Tip: deze periodes zijn geschikt voor tijdsprongen, voice-over of flashbacks.")
    c.sluit()


def cmd_actorprofiel(args):
    """Toont het profiel van een actor."""
    from dagboekmaker.corpus import Corpus
    c = Corpus(args.corpus)
    profiel = c.actor_profiel(args.actor_id)
    if not profiel:
        print(f"Actor '{args.actor_id}' niet gevonden.")
        c.sluit()
        return

    print(f"\n{'='*50}")
    print(f"ACTOR: {profiel.get('naam', '?')}")
    print(f"{'='*50}")
    print(f"ID:              {profiel['id']}")
    print(f"Aliassen:        {', '.join(profiel.get('aliassen', [])) or '—'}")
    print(f"Relatie:         {profiel.get('meest_voorkomende_relatie', '?')}")
    print(f"Documenten:      {profiel.get('aantal_docs', 0)}")
    print(f"Eerste optreden: {profiel.get('eerste_vermelding', '?')}")
    print(f"Laatste optreden:{profiel.get('laatste_vermelding', '?')}")

    # Toon tijdlijn
    tijdlijn = c.actor_tijdlijn(args.actor_id)
    if tijdlijn:
        print(f"\nTijdlijn ({len(tijdlijn)} vermeldingen):")
        for t in tijdlijn[:args.max]:
            rel = f" [{t['relatie']}]" if t.get("relatie") else ""
            print(f"  {t.get('datum_geschat','?'):12}  {t.get('type','?'):10}"
                  f"{rel}  {(t.get('samenvatting') or '')[:50]}")
    c.sluit()


def main():
    parser = argparse.ArgumentParser(
        description="Dagboekmaker — persoonlijk archief naar scriptbasis"
    )
    sub = parser.add_subparsers(dest="commando", required=True)

    # verwerk
    p_verwerk = sub.add_parser("verwerk", help="Verwerk bronbestanden")
    p_verwerk.add_argument("--bron",      required=True, help="Bronmap met archief")
    p_verwerk.add_argument("--corpus",    required=True, help="Outputmap voor corpus")
    p_verwerk.add_argument("--backend",   default="anthropic", choices=["anthropic","ollama"])
    p_verwerk.add_argument("--model",     default=None)
    p_verwerk.add_argument("--glob",      default="**/*")
    p_verwerk.add_argument("--herverwerk",action="store_true")
    p_verwerk.add_argument("--geen-split",action="store_true",
                           help="Splits dagboekteksten niet op datumkoppen")
    p_verwerk.set_defaults(func=cmd_verwerk)

    # stats
    p_stats = sub.add_parser("stats", help="Voortgangsstatistieken")
    p_stats.add_argument("--corpus", required=True)
    p_stats.set_defaults(func=cmd_stats)

    # zoek
    p_zoek = sub.add_parser("zoek", help="Zoek documenten")
    p_zoek.add_argument("--corpus",    required=True)
    p_zoek.add_argument("--type",      default=None)
    p_zoek.add_argument("--periode",   default=None)
    p_zoek.add_argument("--actor",     default=None)
    p_zoek.add_argument("--zekerheid", type=float, default=0.0)
    p_zoek.add_argument("--keerpunten",action="store_true")
    p_zoek.add_argument("--max",       type=int, default=20)
    p_zoek.set_defaults(func=cmd_zoek)

    # tijdlijn
    p_tl = sub.add_parser("tijdlijn", help="Tijdlijn in terminal")
    p_tl.add_argument("--corpus", required=True)
    p_tl.set_defaults(func=cmd_tijdlijn)

    # narratief
    p_narr = sub.add_parser("narratief", help="Narratieve briefing per periode")
    p_narr.add_argument("--corpus",  required=True)
    p_narr.add_argument("--periode", required=True,
                        choices=["kindertijd","adolescentie","jong_volwassen",
                                 "breekpunt","opbouw","heden"])
    p_narr.set_defaults(func=cmd_narratief)

    # gaten
    p_gaten = sub.add_parser("gaten", help="Tijdlijn-gaten (ongedocumenteerde periodes)")
    p_gaten.add_argument("--corpus", required=True)
    p_gaten.add_argument("--min-maanden", type=int, default=6,
                         help="Minimale duur in maanden om als gat te tellen (default: 6)")
    p_gaten.set_defaults(func=cmd_gaten)

    # actorprofiel
    p_actor = sub.add_parser("actorprofiel", help="Profiel en tijdlijn van een actor")
    p_actor.add_argument("--corpus", required=True)
    p_actor.add_argument("--actor-id", required=True, help="Actor-ID (bv. actor_b41264db)")
    p_actor.add_argument("--max", type=int, default=20)
    p_actor.set_defaults(func=cmd_actorprofiel)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
