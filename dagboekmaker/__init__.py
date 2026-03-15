"""
Dagboekmaker — persoonlijk archief naar scriptbasis.
"""
from .pipeline import Pipeline
from .corpus import Corpus
from .extractor import extraheer
from .datering import dateer_lokaal
from .verrijking import maak_verrijker

__version__ = "0.1.0"
__all__ = ["Pipeline", "Corpus", "extraheer", "dateer_lokaal", "maak_verrijker"]
