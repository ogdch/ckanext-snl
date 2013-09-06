from lxml import etree
from oaipmh.metadata import MetadataReader

class XMLMetadataReader(object):
    """
    A implementation of a metadta reader that returns XML as an object.
    """
    def __call__(self, element):
        return element

class XMLStringMetadataReader(object):
    """
    A implementation of a metadta reader that returns XML as a string.
    """
    def __call__(self, element):
        return etree.tostring(element)

