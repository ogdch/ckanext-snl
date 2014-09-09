from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader
from metadata import XMLMetadataReader
from oaipmh.server import oai_dc_writer
from lxml import etree
from resumption import ResumptionClient
import os
import tempfile
import shutil
import datetime
import s3
import logging
log = logging.getLogger(__name__)


class OAI():

    def __init__(
            self,
            bucket_prefix,
            url='http://opac.admin.ch/cgi-bin/nboai/VTLS/Vortex.pl',
            metadata_prefix='marcxml'):
        self.registry = MetadataRegistry()
        self.url = url

        if metadata_prefix == 'marcxml':
            reader = XMLMetadataReader()
            self.registry.registerReader(metadata_prefix, reader)
        elif metadata_prefix == 'oai_dc':
            reader = oai_dc_reader
            writer = oai_dc_writer
            self.registry.registerReader(metadata_prefix, reader)
            self.registry.registerWriter(metadata_prefix, writer)
        else:
            log.debug('Metadata format not marcxml or oai_dc')

        self.client = Client(self.url, self.registry)
        self.s3 = s3.S3()
        self.bucket_prefix = bucket_prefix

    def _concatenate_xml_files(
            self,
            output_filename,
            filenames,
            wrap='records'):
        with open(output_filename, 'w') as outfile:
            if wrap is not None:
                outfile.write('<' + wrap + '>\n')
            for filename in filenames:
                with open(filename) as infile:
                    outfile.write(infile.read())
            if wrap is not None:
                outfile.write('</' + wrap + '>')

    def _upload_dir_content_to_s3(self, set_name, dir_name):
        bucket_name = self.bucket_prefix + '.' + set_name
        self.s3.upload_dir_to_bucket(bucket_name, dir_name)

    def _upload_file_to_s3(self, set_name, dir_name, filename):
        bucket_name = self.bucket_prefix + '.' + set_name
        self.s3.upload_file_to_bucket(bucket_name, dir_name, filename)

    def _dump_s3_bucket_to_dir(self, set_name, dir_name, ignore=None):
        prefix = self.bucket_prefix + '.' + set_name + '/'
        return self.s3.download_bucket_to_dir(prefix, dir_name, ignore)

    def _get_url_of_file(self, set_name, filename):
        bucket_name = self.bucket_prefix + '.' + set_name
        return self.s3.get_url_of_file(bucket_name, filename)

    def get_size_of_file(self, set_name, filename):
        bucket_name = self.bucket_prefix + '.' + set_name
        return self.s3.get_size_of_file(bucket_name, filename)

    def dump(self, set_name):
        temp_dir = tempfile.mkdtemp()
        print self._dump_s3_bucket_to_dir(set_name, temp_dir)

    def export(
            self,
            set_name,
            append=False,
            params=None,
            count=0,
            limit=None,
            export_filename='records.xml',
            metadata_prefix='marcxml'):
        log.debug('Starting to export set %s' % set_name)
        log.debug('oai_url: ' + self.url)
        actual_set_name = set_name

        if params is None:
            actual_set_name = set_name if not append else 'NewBib'
            params = {
                'set': actual_set_name,
                'metadataPrefix': metadata_prefix,
            }

        filenames = []
        step_files = []
        temp_dir = tempfile.mkdtemp()
        log.debug('Temporary directory created: %s' % temp_dir)

        if (append and limit is None):
            log.debug('Copy content from bucket to append new data...')
            step_files = self._dump_s3_bucket_to_dir(
                set_name,
                temp_dir,
                ignore=[export_filename]
            )
            prev_export_file = os.path.join(temp_dir, export_filename)
            try:
                step_files.remove(prev_export_file)
            except ValueError:
                pass

        log.debug('Params: %s' % params)
        for header, metadata, about in self.client.listRecords(**params):
            # check if we use NewBib to add only new entries
            if (actual_set_name != set_name and set_name != 'NewBib'):
                try:
                    xml_namespaces = {
                        'marc': 'http://www.loc.gov/MARC21/slim',
                    }
                    record_type = etree.XPath(
                        (
                            ".//marc:datafield[@tag='993']"
                            "/marc:subfield[@code='a']"
                        ),
                        namespaces=xml_namespaces
                    )
                    if record_type(metadata)[0].text != set_name:
                        log.debug(
                            'Record does not belong to set %s'
                            % set_name
                        )
                        continue
                except IndexError:
                    log.debug('Record does not belong to set %s' % set_name)
                    continue

            count += 1
            today = datetime.date.today().strftime("%Y-%m-%d")
            log.debug(
                'Fetching record %s from set %s: %s'
                % (count, set_name, header.identifier())
            )

            # save record as XML file
            newRecord = etree.Element("record")
            if metadata_prefix == 'marcxml':
                newRecord.append(metadata)
            else:
                self.registry.writeMetadata(
                    metadata_prefix,
                    newRecord,
                    metadata
                )
            tree = etree.ElementTree(newRecord)
            filename = os.path.join(temp_dir, header.identifier() + '.xml')
            filenames.append(filename)
            tree.write(filename, pretty_print=True)

            if (count % 500 == 0):
                log.debug('Create step file %s' % count)
                step_files.append(
                    self._create_step_file(
                        str(count) + '_' + today,
                        temp_dir,
                        set_name, filenames
                    )
                )
                filenames = []

            if (limit is not None and count >= limit):
                break

        if filenames:
            today = datetime.date.today().strftime("%Y-%m-%d")
            log.debug('Create step file %s' % count)
            step_files.append(
                self._create_step_file(
                    str(count) + '_' + today,
                    temp_dir,
                    set_name,
                    filenames
                )
            )

        if (limit is not None):
            return step_files

        record_filename = os.path.join(temp_dir, export_filename)
        log.debug('Record file: %s' % record_filename)
        self._concatenate_xml_files(record_filename, step_files)

        if (append):
            log.debug('Uploading dir %s to S3' % temp_dir)
            self._upload_dir_content_to_s3(set_name, temp_dir)
        else:
            log.debug(
                'Uploading %s from %s to S3'
                % (export_filename, temp_dir)
            )
            self._upload_file_to_s3(set_name, temp_dir, export_filename)
        log.debug('Deleting directory ' + temp_dir)
        try:
            shutil.rmtree(temp_dir)
        except Exception, e:
            log.exception(e)
            pass

        return self._get_url_of_file(set_name, export_filename)

    def _create_step_file(self, step_name, dir_name, set_name, filenames):
            step_file = os.path.join(
                dir_name,
                set_name + '_' + step_name + '.xml_part'
            )
            self._concatenate_xml_files(step_file, filenames, wrap=None)
            for filename in filenames:
                try:
                    os.remove(filename)
                    log.debug('Deleted file %s' % (filename))
                except OSError:
                    log.debug('Could not delete file %s' % (filename))
            return step_file

    def resume_export(self, set_name, append, count, limit):
        params = {
            'resumptionToken': set_name + '|marcxml|' + str(count) + '|||',
        }
        self.client = ResumptionClient(self.url, 'marcxml', self.registry)
        self.export(set_name, append, params, count, limit)
