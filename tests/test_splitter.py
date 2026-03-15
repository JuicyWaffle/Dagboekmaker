"""Tests voor dagboekmaker.splitter — dagsplitsing op datumkoppen."""

import pytest
from dagboekmaker.splitter import splits_dagboek, Fragment, SplitResultaat


class TestMeerdereEntries:
    def test_drie_datumkoppen_geeft_drie_fragmenten(self):
        tekst = (
            "14 maart 1995\n"
            "Vandaag was een mooie dag. Ik ging naar school en leerde veel over wiskunde.\n\n"
            "15 maart 1995\n"
            "Het regende de hele dag. Mama was verdrietig over het nieuws van tante Riet.\n\n"
            "16 maart 1995\n"
            "Papa kwam thuis van het werk met een verrassing voor ons allemaal.\n"
        )
        result = splits_dagboek(tekst)
        assert result.is_gesplitst
        assert result.methode == "datum_headers"
        assert len(result.fragmenten) == 3
        assert result.fragmenten[0].datum_header == "14 maart 1995"
        assert result.fragmenten[1].datum_header == "15 maart 1995"
        assert result.fragmenten[2].datum_header == "16 maart 1995"
        assert "mooie dag" in result.fragmenten[0].tekst
        assert "regende" in result.fragmenten[1].tekst
        assert "verrassing" in result.fragmenten[2].tekst

    def test_volgnummers_zijn_oplopend(self):
        tekst = (
            "1 januari 2000\nEntry een, het begin van een nieuw millennium.\n\n"
            "2 januari 2000\nEntry twee, de wereld draait nog steeds.\n\n"
            "3 januari 2000\nEntry drie, alles is rustig op kantoor.\n"
        )
        result = splits_dagboek(tekst)
        for i, frag in enumerate(result.fragmenten):
            assert frag.volgnummer == i

    def test_posities_kloppen(self):
        tekst = (
            "14 maart 1995\n"
            "Eerste entry met voldoende tekst om niet samengevoegd te worden.\n\n"
            "15 maart 1995\n"
            "Tweede entry, ook lang genoeg voor een apart fragment in de output.\n"
        )
        result = splits_dagboek(tekst)
        assert result.fragmenten[0].positie_start == 0
        for i in range(1, len(result.fragmenten)):
            assert result.fragmenten[i].positie_start >= result.fragmenten[i - 1].positie_start


class TestGeenDatums:
    def test_platte_tekst_geeft_een_fragment(self):
        tekst = "Dit is gewoon wat tekst zonder enige datumvermelding erin."
        result = splits_dagboek(tekst)
        assert not result.is_gesplitst
        assert result.methode == "geen_split"
        assert len(result.fragmenten) == 1
        assert result.fragmenten[0].tekst == tekst

    def test_lege_tekst(self):
        result = splits_dagboek("")
        assert not result.is_gesplitst
        assert len(result.fragmenten) == 1

    def test_none_achtige_tekst(self):
        result = splits_dagboek("   \n\n  ")
        assert not result.is_gesplitst
        assert len(result.fragmenten) == 1


class TestEnkeleDatum:
    def test_een_datumkop_geen_split(self):
        tekst = (
            "14 maart 1995\n"
            "Dit is het enige dagboekfragment in dit bestand.\n"
            "Verder niets bijzonders te melden vandaag.\n"
        )
        result = splits_dagboek(tekst)
        assert not result.is_gesplitst
        assert len(result.fragmenten) == 1
        assert result.fragmenten[0].datum_header == "14 maart 1995"


class TestProloog:
    def test_tekst_voor_eerste_datum_wordt_proloog(self):
        tekst = (
            "Dagboek van George Vayssier\n"
            "Geschreven in Wenen, 1995\n\n"
            "14 maart 1995\n"
            "Eerste entry. Vandaag begon ik met schrijven in dit dagboek.\n\n"
            "15 maart 1995\n"
            "Tweede entry. Het gaat goed, het weer is prachtig in Wenen.\n"
        )
        result = splits_dagboek(tekst)
        assert result.is_gesplitst
        assert len(result.fragmenten) == 3
        assert result.fragmenten[0].datum_header is None  # proloog
        assert "Dagboek van George" in result.fragmenten[0].tekst
        assert result.fragmenten[0].volgnummer == 0
        assert result.fragmenten[1].datum_header == "14 maart 1995"
        assert result.fragmenten[1].volgnummer == 1


class TestNumeriekeDatums:
    def test_dd_mm_yyyy_slash(self):
        tekst = (
            "14/03/1995\n"
            "Eerste dag met het nieuwe dagboek. Veel te vertellen over vandaag.\n\n"
            "15/03/1995\n"
            "Tweede dag, het regent maar ik voel me goed over de toekomst.\n"
        )
        result = splits_dagboek(tekst)
        assert result.is_gesplitst
        assert len(result.fragmenten) == 2

    def test_dd_mm_yyyy_streepje(self):
        tekst = (
            "14-03-1995\n"
            "Eerste entry in het dagboek, veel gedachten over het verleden.\n\n"
            "15-03-1995\n"
            "Tweede entry, de dag verliep rustig en zonder incidenten.\n"
        )
        result = splits_dagboek(tekst)
        assert result.is_gesplitst
        assert len(result.fragmenten) == 2

    def test_dd_mm_yyyy_punt(self):
        tekst = (
            "14.03.1995\n"
            "Eerste entry, een bijzonder mooie dag in Wenen vandaag.\n\n"
            "15.03.1995\n"
            "Tweede entry, het weer sloeg om maar het humeur bleef goed.\n"
        )
        result = splits_dagboek(tekst)
        assert result.is_gesplitst
        assert len(result.fragmenten) == 2


class TestWeekdagPrefix:
    def test_weekdag_met_datum(self):
        tekst = (
            "Dinsdag 14 maart 1995\n"
            "Vandaag ging ik naar school en leerde veel over geschiedenis.\n\n"
            "Woensdag 15 maart 1995\n"
            "Het regende de hele dag, ik bleef binnen en las een boek.\n"
        )
        result = splits_dagboek(tekst)
        assert result.is_gesplitst
        assert len(result.fragmenten) == 2

    def test_engelse_weekdag(self):
        tekst = (
            "Tuesday 14 march 1995\n"
            "Went to school today, learned about the French Revolution.\n\n"
            "Wednesday 15 march 1995\n"
            "It rained all day long, stayed inside reading a novel.\n"
        )
        result = splits_dagboek(tekst)
        assert result.is_gesplitst
        assert len(result.fragmenten) == 2


class TestInlineDatum:
    def test_datum_midden_in_zin_geen_header(self):
        tekst = (
            "Ik herinnerde me 14 maart 1995 als de dag dat alles veranderde. "
            "Het was een prachtige dag, de zon scheen en de vogels zongen. "
            "Niets wees erop dat het leven zo zou kantelen."
        )
        result = splits_dagboek(tekst)
        assert not result.is_gesplitst

    def test_datum_op_lange_regel_geen_header(self):
        tekst = (
            "Op 14 maart 1995 ging ik naar de markt om groenten te kopen "
            "voor het avondeten met de hele familie.\n"
            "Later die dag belde Rosa Maria om te vragen of ik langskwam.\n"
        )
        result = splits_dagboek(tekst)
        assert not result.is_gesplitst


class TestKortFragmentMerge:
    def test_kort_fragment_wordt_samengevoegd(self):
        tekst = (
            "14 maart 1995\n"
            "Lang verhaal over de dag, met veel details over het weer,\n"
            "de school en het avondeten. Mama maakte stamppot.\n\n"
            "15 maart 1995\n"
            "Kort.\n\n"
            "16 maart 1995\n"
            "Weer een normaal lang verhaal over de dag, met details.\n"
            "Het regende en ik bleef binnen met een goed boek.\n"
        )
        result = splits_dagboek(tekst)
        assert result.is_gesplitst
        # "15 maart" is <20 tekens body, wordt samengevoegd met "14 maart"
        assert len(result.fragmenten) == 2
        assert "Kort." in result.fragmenten[0].tekst
        assert "14 maart 1995" in result.fragmenten[0].tekst

    def test_custom_min_fragment_len(self):
        tekst = (
            "14 maart 1995\nA.\n\n"
            "15 maart 1995\nB.\n"
        )
        # Met min_fragment_len=1 worden zelfs korte fragmenten behouden
        result = splits_dagboek(tekst, min_fragment_len=1)
        assert result.is_gesplitst
        assert len(result.fragmenten) == 2


class TestDatumZonderJaar:
    def test_datum_zonder_jaar_wordt_herkend(self):
        tekst = (
            "14 maart\n"
            "Vandaag was het mooi weer. Ik wandelde door het park.\n\n"
            "15 maart\n"
            "Het regende weer eens. Rosa Maria belde vanuit Wenen.\n"
        )
        result = splits_dagboek(tekst)
        assert result.is_gesplitst
        assert len(result.fragmenten) == 2

    def test_verkort_jaar(self):
        tekst = (
            "14 maart '95\n"
            "Eerste entry in het dagboek, een bijzondere dag vandaag.\n\n"
            "15 maart '95\n"
            "Tweede entry, het leven gaat verder in Wenen met Rosa Maria.\n"
        )
        result = splits_dagboek(tekst)
        assert result.is_gesplitst
        assert len(result.fragmenten) == 2


class TestGemengdeFormaten:
    def test_mix_lang_en_numeriek(self):
        tekst = (
            "14 maart 1995\n"
            "Entry in lang formaat, een bijzondere dag voor het dagboek.\n\n"
            "15/03/1995\n"
            "Entry in numeriek formaat, ook een belangrijke dag geweest.\n"
        )
        result = splits_dagboek(tekst)
        assert result.is_gesplitst
        assert len(result.fragmenten) == 2


class TestSplitResultaat:
    def test_dataclass_velden(self):
        result = splits_dagboek("test")
        assert hasattr(result, "fragmenten")
        assert hasattr(result, "is_gesplitst")
        assert hasattr(result, "methode")

    def test_fragment_dataclass_velden(self):
        result = splits_dagboek("test")
        frag = result.fragmenten[0]
        assert hasattr(frag, "volgnummer")
        assert hasattr(frag, "tekst")
        assert hasattr(frag, "datum_header")
        assert hasattr(frag, "positie_start")
        assert hasattr(frag, "positie_eind")
