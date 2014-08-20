# -*- coding: utf-8 -*-

import os
import tempfile
import urllib3
import shutil

from ckan import model
from ckan.model import Session
from ckan.logic import get_action, action
from ckan.lib.helpers import json
from ckan.lib.munge import munge_title_to_name

from ckanext.harvest.model import HarvestObject
from ckanext.harvest.harvesters import HarvesterBase

from ckanext.snl.helpers import oai
from ckanext.snl.helpers.xml_metadata import MetaDataParser

import logging
log = logging.getLogger(__name__)


class SNLHarvester(HarvesterBase):
    '''
    The harvester for snl
    '''

    HARVEST_USER = u'harvest'

    METADATA_FILE_URL = 'http://ead.nb.admin.ch/ogd/OGD_Metadaten_NB.xml'
    METADATA_FILE_NAME = 'OGD_Metadaten_NB.xml'

    ORGANIZATION = {
        u'de': {
            'name': u'Schweizerische Nationalbibliothek',
            'description': (
                u'Sammelt alle Schweizer Publikationen seit 1848. Ebenfalls '
                u'zur Nationalbibliothek gehören das Schweizerische '
                u'Literaturarchiv, die Graphische Sammlung und das '
                u'Centre Dürrenmatt.'
            ),
            'website': 'http://www.nb.admin.ch/'
        },
        u'fr': {
            'name': u'Bibliothèque nationale suisse',
            'description': (
                u'Collecte l’ensemble des publications suisses depuis '
                u'1848. Les Archives littéraires suisses, le Cabinet des '
                u'estampes et le Centre Dürrenmatt font également partie '
                u'de la Bibliothèque nationale.'
            )
        },
        u'it': {
            'name': u'Biblioteca nazionale svizzera',
            'description': (
                u'Colleziona tutte le pubblicazioni a partire dal 1848. '
                u'Alla Biblioteca nazionale sono accorpati l\'Archivio '
                u'svizzero di letteratura, il Gabinetto delle stampe e '
                u'il Centre Dürrenmatt.'
            )
        },
        u'en': {
            'name': u'Swiss National Library',
            'description': (
                u'Collects all Swiss publications since 1848. The Swiss '
                u'Literary Archives, the Cabinet of Prints and Drawings '
                u'and the Centre Dürrenmatt also belong to the '
                u'National Library.'
            )
        }
    }

    GROUPS = {
        u'de': [u'Bildung und Wissenschaft'],
        u'fr': [u'Education et science'],
        u'it': [u'Formazione e scienza'],
        u'en': [u'Education and science']
    }

    def _fetch_metadata_file(self):
        '''
        Fetching the Excel metadata file from the NB and save on disk
        '''
        http = urllib3.PoolManager()

        log.debug('Fetch metadata file from %s' % self.METADATA_FILE_URL)
        temp_dir = tempfile.mkdtemp()
        local_path = os.path.join(temp_dir, self.METADATA_FILE_NAME)
        metadata_file = http.request('GET', self.METADATA_FILE_URL)
        with open(local_path, 'w') as local_file:
            local_file.write(metadata_file.data)
        return local_path

    def info(self):
        return {
            'name': 'snl',
            'title': 'SNL',
            'description': 'Harvests the snl data',
            'form_config_interface': 'Text'
        }

    def gather_stage(self, harvest_job):
        log.debug('In SNLHarvester gather_stage')

        metadata_path = self._fetch_metadata_file()
        ids = []

        parser = MetaDataParser(metadata_path)

        for dataset in parser.list_datasets():
            metadata = parser.parse_set(dataset)
            metadata['translations'].extend(self._metadata_term_translations())

            log.debug(metadata)

            obj = HarvestObject(
                guid=metadata['id'],
                job=harvest_job,
                content=json.dumps(metadata)
            )
            obj.save()
            log.debug('adding ' + metadata['id'] + ' to the queue')
            ids.append(obj.id)

        temp_dir = os.path.dirname(metadata_path)
        shutil.rmtree(temp_dir)

        return ids

    def fetch_stage(self, harvest_object):
        log.debug('In SNLHarvester fetch_stage')
        package_dict = json.loads(harvest_object.content)
        bucket_prefix = package_dict['bucket_prefix']
        append = True if package_dict['append_data'] == u'True' else False

        for resource in package_dict['resources']:
            oai_url = resource['oai_url']
            metadata_prefix = resource['metadata_prefix']
            oai_helper = oai.OAI(bucket_prefix, oai_url, metadata_prefix)
            if resource['type'] == 'oai':
                record_file_url = oai_helper.export(
                    package_dict['id'],
                    append=append,
                    export_filename=resource['export_filename'],
                    metadata_prefix=metadata_prefix
                )
                log.debug('Record file URL: %s' % record_file_url)
                resource['url'] = record_file_url
                resource['size'] = oai_helper.get_size_of_file(
                    package_dict['id'],
                    resource['export_filename']
                )
                log.debug('Size added to resource.')
            else:
                resource['size'] = oai_helper.get_size_of_file(
                    package_dict['id'],
                    resource['export_filename']
                )
                log.debug('Size added to resource.')

        harvest_object.content = json.dumps(package_dict)
        harvest_object.save()

        return True

    def import_stage(self, harvest_object):
        log.debug('In SNLHarvester import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            return False

        try:
            package_dict = json.loads(harvest_object.content)

            user = model.User.get(self.HARVEST_USER)

            context = {
                'model': model,
                'session': Session,
                'user': self.HARVEST_USER
            }

            # Find or create group the dataset should get assigned to
            package_dict['groups'] = self._find_or_create_groups(context)

            # Find or create the organization
            # the dataset should get assigned to
            package_dict['owner_org'] = self._find_or_create_organization(
                context
            )

            # Never import state from data source!
            if 'state' in package_dict:
                del package_dict['state']

            if 'extra_harvester_url' in package_dict:
                del package_dict['extra_harvester_url']

            # Split tags
            tags = package_dict.get('tags', '').split(',')
            tags = [tag.strip() for tag in tags]
            package_dict['tags'] = tags

            package = model.Package.get(package_dict['id'])
            model.PackageRole(
                package=package,
                user=user,
                role=model.Role.ADMIN
            )

            log.debug('Save or update package %s' % (package_dict['name'],))
            self._create_or_update_package(package_dict, harvest_object)

            log.debug('Save or update term translations')
            self._submit_term_translations(context, package_dict)

            Session.commit()

            log.debug('Importing finished.')
        except Exception, e:
            log.exception(e)
            raise e
        return True

    def _find_or_create_groups(self, context):
        group_name = self.GROUPS['de'][0]
        data_dict = {
            'id': group_name,
            'name': munge_title_to_name(group_name),
            'title': group_name
            }
        try:
            group = get_action('group_show')(context, data_dict)
        except:
            group = get_action('group_create')(context, data_dict)
            log.info('created the group ' + group['id'])
        group_ids = []
        group_ids.append(group['id'])
        return group_ids

    def _find_or_create_organization(self, context):
        data_dict = {
            'permission': 'edit_group',
            'id': munge_title_to_name(self.ORGANIZATION['de']['name']),
            'name': munge_title_to_name(self.ORGANIZATION['de']['name']),
            'title': self.ORGANIZATION['de']['name'],
            'description': self.ORGANIZATION['de']['description'],
            'extras': [
                {
                    'key': 'website',
                    'value': self.ORGANIZATION['de']['website']
                }
            ]
        }
        try:
            organization = get_action('organization_show')(context, data_dict)
        except:
            organization = get_action('organization_create')(
                context,
                data_dict
            )
        return organization['id']

    def _metadata_term_translations(self):
        '''
        Generate term translatations for organizations
        '''
        try:
            translations = []

            for lang, org in self.ORGANIZATION.items():
                if lang != u'de':
                    for field in ['name', 'description']:
                        translations.append({
                            'lang_code': lang,
                            'term': self.ORGANIZATION['de'][field],
                            'term_translation': org[field]
                        })

            for lang, groups in self.GROUPS.iteritems():
                if lang != u'de':
                    for idx, group in enumerate(self.GROUPS[lang]):
                        translations.append({
                            'lang_code': lang,
                            'term': self.GROUPS[u'de'][idx],
                            'term_translation': group
                        })

            return translations

        except Exception, e:
            log.exception(e)
            return []

    def _submit_term_translations(self, context, package_dict):
        for translation in package_dict['translations']:
            action.update.term_translation_update(context, translation)
