#n -*- coding: utf-8 -*-

import random
import os
import shutil
import tempfile
import zipfile
from pprint import pprint
from collections import defaultdict
import urllib3

from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action, action
from ckan.lib.helpers import json

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, HarvestObjectError
from ckanext.harvest.harvesters import HarvesterBase

from ckanext.snl.helpers import s3
from ckanext.snl.helpers import oai
from ckanext.snl.helpers.xls_metadata import MetaDataParser

import logging
log = logging.getLogger(__name__)

class SNLHarvester(HarvesterBase):
    '''
    The harvester for snl
    '''

    HARVEST_USER = u'harvest'

    METADATA_FILE_URL = 'http://ead.nb.admin.ch/ogd/OGD_Metadaten_NB.xlsx'
    METADATA_FILE_NAME = 'OGD_Metadaten_NB.xlsx'

    SHEETS = (
        (u'NB-BSG', u'NewBib', True, 'http://opac.admin.ch/cgi-bin/biblioai/VTLS/Vortex.pl'),
        (u'NB-SB', u'sb', True, 'http://opac.admin.ch/cgi-bin/nboai/VTLS/Vortex.pl'),
        (u'NB-e-diss', u'e-diss', False, 'http://opac.admin.ch/cgi-bin/nboai/VTLS/Vortex.pl'),
        (u'NB-digicoll', 'digicoll', False, 'http://opac.admin.ch/cgi-bin/nboai/VTLS/Vortex.pl'),
    )

    ORGANIZATION = {
        u'de': u'Schweizerische Nationalbibliothek',
        u'fr': u'Bibliothèque nationale suisse',
        u'it': u'Biblioteca nazionale svizzera',
        u'en': u'Swiss National Library',
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
        metadata_file = http.request('GET', self.METADATA_FILE_URL)
        with open(self.METADATA_FILE_NAME, 'w') as local_file:
            local_file.write(metadata_file.data)


    def info(self):
        return {
            'name': 'snl',
            'title': 'SNL',
            'description': 'Harvests the snl data',
            'form_config_interface': 'Text'
        }

    def gather_stage(self, harvest_job):
        log.debug('In SNLHarvester gather_stage')

        self._fetch_metadata_file()
        ids = []
        for sheet_name, set_name, appenad, oai_url in self.SHEETS:
            log.debug('Gathering %s' % sheet_name)

            parser = MetaDataParser(self.METADATA_FILE_NAME)

            metadata = parser.parse_sheet(sheet_name)
            metadata['translations'].extend(self._metadata_term_translations())
            metadata['sheet_name'] = sheet_name
            metadata['set_name'] = set_name
            metadata['append_data'] = append
            metadata['oai_url'] = oai_url
            log.debug(metadata)

            obj = HarvestObject(
                #guid = metadata.get('id'),
                job = harvest_job,
                content = json.dumps(metadata)
            )

            obj.save()
            ids.append(obj.id)

        return ids 


    def fetch_stage(self, harvest_object):
        log.debug('In SNLHarvester fetch_stage')
        package_dict = json.loads(harvest_object.content)

        #oai_url = package_dict.get('extra_harvester_url', 'http://opac.admin.ch/cgi-bin/nboai/VTLS/Vortex.pl')
        oai_url = package_dict['oai_url']
        oai_helper = oai.OAI('ch.nb', oai_url)
        record_file_url = oai_helper.export(package_dict['set_name'], package_dict['append_data'])
        log.debug('Record file URL: %s' % record_file_url)
        package_dict['resources'][0]['url'] = record_file_url

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

            # Find or create the organization the dataset should get assigned to
            package_dict['owner_org'] = self._find_or_create_organization(context)

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
            model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

            #log.debug('Save or update package %s' % (package_dict['name'],))
            result = self._create_or_update_package(package_dict, harvest_object)

            log.debug('Save or update term translations')
            self._submit_term_translations(context, package_dict)

            Session.commit()
        except Exception, e:
            log.exception(e)
            raise e
        return True

    def _find_or_create_groups(self, context):
        group_name = self.GROUPS['de'][0]
        data_dict = {
            'id': group_name,
            'name': self._gen_new_name(group_name),
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
        try:
            data_dict = {
                'permission': 'edit_group',
                'id': self._gen_new_name(self.ORGANIZATION[u'de']),
                'name': self._gen_new_name(self.ORGANIZATION[u'de']),
                'title': self.ORGANIZATION[u'de']
            }
            organization = get_action('organization_show')(context, data_dict)
        except:
            organization = get_action('organization_create')(context, data_dict)
        return organization['id']

    def _metadata_term_translations(self):
        '''
        Generate term translatations for organizations
        '''
        try:
            translations = []

            for lang, org in self.ORGANIZATION.items():
                if lang != u'de':
                    translations.append({
                        'lang_code': lang,
                        'term': self.ORGANIZATION[u'de'],
                        'term_translation': org
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
