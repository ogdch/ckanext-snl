import ckan
import ckan.plugins as p
from pylons import config

class SNLHarvest(p.SingletonPlugin):
    """
    Plugin containg the harvester for Swiss National Library
    """
