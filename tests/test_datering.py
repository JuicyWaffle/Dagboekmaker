"""
Basistest voor de dateringsmodule.
Draai met: pytest tests/
"""
import pytest
from dagboekmaker.datering import dateer_lokaal, DatumOnzekerheid


def test_volledige_datum():
    d = dateer_lokaal("Brief van 14 maart 1995.")
    assert d.dag == 14
    assert d.maand == 3
    assert d.jaar_min == d.jaar_max == 1995
    assert d.zekerheid >= 0.85


def test_dag_maand_zonder_jaar():
    d = dateer_lokaal("Zondag 14 maart, lieve mama")
    assert d.dag == 14
    assert d.maand == 3
    assert d.dag_van_week == "zondag"
    # Jaar onbekend maar kandidaten ingevuld
    assert len(d.jaar_kandidaten) > 0
    # 1995 moet erin zitten (14 maart 1995 was een dinsdag — dus NIET)
    # 14 maart 2004 was een zondag
    assert 2004 in d.jaar_kandidaten


def test_numerieke_datum():
    d = dateer_lokaal("Zie bijlage van 05/08/1988.")
    assert d.dag == 5
    assert d.maand == 8
    assert d.jaar_min == d.jaar_max == 1988


def test_geen_datum():
    d = dateer_lokaal("Hallo, hoe gaat het met jou?")
    assert d.zekerheid < 0.5
    assert d.dag is None
    assert d.maand is None


def test_bestandsdatum_prior():
    d = dateer_lokaal("Geen datuminformatie.", bestandsdatum="1997-06-01")
    assert d.jaar_max <= 1998  # bestandsdatum +1 als bovengrens
    assert d.zekerheid >= 0.15


def test_actor_constraint(monkeypatch):
    from dagboekmaker.datering import GlobaleDateringsmotor
    actors = {
        "actor_vader": {
            "naam": "Jef",
            "overlijden": "1998-09-03",
            "geboorte": "1944",
        }
    }
    motor = GlobaleDateringsmotor(actors)
    d = DatumOnzekerheid(dag=14, maand=3, jaar_min=1990, jaar_max=2005,
                          zekerheid=0.4, datum_geschat="1990–2005")
    doc_actors = [{"ref": "actor_vader", "rol": "vermeld"}]
    d2 = motor.pas_actor_constraints_toe("doc_test", doc_actors, d)
    assert d2.jaar_max <= 1998


def test_datum_onzekerheid_precisie():
    d = DatumOnzekerheid(dag=5, maand=8, jaar_min=1988, jaar_max=1988, zekerheid=0.9)
    d.datum_geschat = "1988-08-05"
    assert d._precisie() == "dag"
    assert d.is_opgelost()


def test_redenering_log():
    d = dateer_lokaal("Dinsdag 14 maart 1995, beste vriend")
    # Moet minstens één redenering bevatten
    assert len(d.redenering) >= 1
    # Moet minstens één versie in de geschiedenis hebben
    assert len(d.geschiedenis) >= 1
