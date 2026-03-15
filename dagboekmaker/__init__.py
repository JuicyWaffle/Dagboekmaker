"""
Dagboekmaker — persoonlijk archief naar scriptbasis.
"""
from .pipeline import Pipeline
from .corpus import Corpus
from .extractor import extraheer
from .datering import dateer_lokaal
from .splitter import splits_dagboek
from .verrijking import maak_verrijker

__version__ = "0.2.0"
__all__ = ["Pipeline", "Corpus", "extraheer", "dateer_lokaal", "splits_dagboek", "maak_verrijker"]
