from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry
from metadata import XMLMetadataReader
from lxml import etree
from StringIO import StringIO
from resumption import ResumptionClient
import os
import tempfile
import shutil
import datetime
import s3
import logging
log = logging.getLogger(__name__)

class OAI():

    def __init__(self, bucket_prefix, url='http://opac.admin.ch/cgi-bin/nboai/VTLS/Vortex.pl'):
        self.registry = MetadataRegistry()
        self.url = url
        xml_reader = XMLMetadataReader()
        self.registry.registerReader('marcxml', xml_reader)
        self.client = Client(self.url, self.registry)
        self.s3 = s3.S3()
        self.bucket_prefix = bucket_prefix

    def _concatenate_xml_files(self, output_filename, filenames, wrap='records'):
        with open(output_filename, 'w') as outfile:
            if wrap is not None:
                outfile.write('<' + wrap + '\n');
            for filename in filenames:
                with open(filename) as infile:
                    outfile.write(infile.read())
            if wrap is not None:
                outfile.write('</' + wrap + '>');

    def _upload_dir_content_to_s3(self, set_name, dir_name):
        bucket_name = self.bucket_prefix + '.' + set_name
        self.s3.upload_dir_to_bucket(bucket_name, dir_name)

    def _upload_file_to_s3(self, set_name, dir_name, filename):
        bucket_name = self.bucket_prefix + '.' + set_name
        self.s3.upload_file_to_bucket(bucket_name, dir_name, filename)

    def _dump_s3_bucket_to_dir(self, set_name, dir_name):
        bucket_name = self.bucket_prefix + '.' + set_name
        return self.s3.download_bucket_to_dir(bucket_name, dir_name)

    def dump(self, set_name):
        temp_dir = tempfile.mkdtemp()
        print self._dump_s3_bucket_to_dir(set_name, temp_dir)

    def export(self, set_name, append=False, params=None, count=0, limit=None, export_filename='records.xml'):
        if params is None:
            params = {
                'set': set_name,
                'metadataPrefix': 'marcxml',
            }

        filenames = []
        step_files = []
        temp_dir = tempfile.mkdtemp()
        log.debug('Temporary directory created: %s' % temp_dir)

        if (append):
            step_files = self._dump_s3_bucket_to_dir(set_name, temp_dir)
            prev_export_file = os.path.join(temp_dir, export_filename)
            try:
                step_files.remove(prev_export_file)
            except ValueError:
                pass

        for header, metadata, about in self.client.listRecords(**params):
            count += 1
            today = datetime.date.today().strftime("%Y-%m-%d")
            print count

            newRecord = etree.Element("record")
            newRecord.append(metadata)
            tree = etree.ElementTree(newRecord)
            filename = os.path.join(temp_dir, header.identifier() + '.xml')
            filenames.append(filename) 
            tree.write(filename, pretty_print=True)

            print " ---- "
            if (count % 500 == 0):
                step_files.append(self._create_step_file(str(count) + '_' + today, temp_dir, set_name, filenames))
                filenames = []

            if (limit is not None and count >= limit):
                break

        if filenames:
            today = datetime.date.today().strftime("%Y-%m-%d")
            step_files.append(self._create_step_file(str(count) + '_' + today, temp_dir, set_name, filenames))

        if (limit is None):
            record_filename = os.path.join(temp_dir, export_filename)
            log.debug('Record file: %s' % record_filename)
            self._concatenate_xml_files(record_filename, step_files)

            if (append):
                self._upload_dir_content_to_s3(set_name, temp_dir)
            else:
                self._upload_file_to_s3(set_name, temp_dir, export_filename)
            shutil.rmtree(temp_dir);

    def _create_step_file(self, step_name, dir_name, set_name, filenames):
            step_file = os.path.join(dir_name, set_name + '_' + step_name + '.xml_part')
            self._concatenate_xml_files(step_file, filenames, wrap=None)
            for filename in filenames:
                os.remove(filename)
            return step_file


    def resume_export(self, set_name, append, count, limit):
        params = {
            'resumptionToken': set_name + '|marcxml|' + str(count) + '|||',
        }
        self.client = ResumptionClient(self.url, 'marcxml', self.registry)
        self.export(set_name, append, params, count, limit)

