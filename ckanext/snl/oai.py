from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry
from metadata import XMLMetadataReader
from lxml import etree
from StringIO import StringIO
import os

def concatenate_xml_files(output_filename, filenames, wrap='records'):
    with open(output_filename, 'w') as outfile:
        if wrap is not None:
            outfile.write('<' + wrap + '\n');
        for filename in filenames:
            with open(filename) as infile:
                outfile.write(infile.read())
        if wrap is not None:
            outfile.write('</' + wrap + '>');

URL = 'http://opac.admin.ch/cgi-bin/nboai/VTLS/Vortex.pl'
registry = MetadataRegistry()
xml_reader = XMLMetadataReader()
registry.registerReader('marcxml', xml_reader)
client = Client(URL, registry)

params = {
    'set': 'AllBib',
    'metadataPrefix': 'marcxml',
}

count = 0
filenames = []
step_files = []
for header, metadata, about in client.listRecords(**params):
    count += 1
    print count

    newRecord = etree.Element("record")
    newRecord.append(metadata)
    tree = etree.ElementTree(newRecord)
    filename = 'records/' + header.identifier() + '.xml'
    filenames.append(filename) 
    tree.write(filename, pretty_print=True)

    print " ---- "
    if (count % 500 == 0):
        step_file = 'records/step_' + str(count) + '.xml_part'
        step_files.append(step_file)
        concatenate_xml_files(step_file, filenames, wrap=None)
        for filename in filenames:
            os.remove(filename)
        filenames = []

concatenate_xml_files('records.xml', step_files + filenames)

