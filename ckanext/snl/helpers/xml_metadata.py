from lxml import etree
import logging
log = logging.getLogger(__name__)

from ckanext.harvest.harvesters.base import munge_tag


class MetaDataParser(object):

    # Only these attributes will be imported into dataset
    DATASET_ATTRIBUTES = (
        'name',
        'title',
        'url',
        'notes',
        'author',
        'maintainer',
        'maintainer_email',
        'license_id',
        'tags',
    )

    # Only these attributes will be imported into resource
    RESOURCE_ATTRIBUTES = (
        'url',
        'name',
        'description',
        'type',
        'format'
    )

    def __init__(self, file_name):
        self.file_name = file_name
        self.meta_xml = open(file_name)

    def parse_sheet(self, set_name):
        '''
        Parse one dataset and its resources and return them as dict
        '''

        meta_xml = self.meta_xml
        parser = etree.XMLParser(encoding='utf-8')
        datasets = etree.fromstring(
            meta_xml.read(),
            parser=parser
        ).findall('dataset')

        for dataset in datasets:
            if dataset.get('id') == set_name:
                dataset_attrs = dataset.find('dataset_attributes')
                metadata = {
                    'id': dataset.get('id')
                }

                for attr in self.DATASET_ATTRIBUTES:
                    metadata[attr] = dataset_attrs.find(attr).find('de').text

                log.debug(metadata)

                if 'name' in metadata:
                    metadata['name'] = munge_tag(metadata['name'])
                    metadata['resources'] = self._build_resources_list(dataset)
                    metadata = self._handle_license(metadata)
                    metadata['translations'] = self._build_term_translations(
                        dataset
                    )

                log.debug(metadata)

                return metadata

    def _clean_values(self, values):
        '''
        Strip whitespace from all strings in values
        '''
        cleaned = []
        for value in values:
            if isinstance(value, basestring):
                value = value.strip()
            cleaned.append(value)

        return cleaned

    def _build_resources_list(self, dataset):
        '''
        Create a list of all resources in the dataset
        '''
        resources_list = []
        resources = dataset.findall('resource')
        for resource in resources:
            current = {}
            for attribute in self.RESOURCE_ATTRIBUTES:
                current[attribute] = resource.find('resource_attributes')\
                    .find(attribute).find('de').text
            resources_list.append(current)

        return resources_list

    def _handle_license(self, metadata):
        if (metadata['license_id'] == 'CCO'):
            metadata['license_id'] = 'cc-zero'

        return metadata

    def _build_term_translations(self, dataset):
        """
        Generate meaningful term translations for all translated values
        """
        translations = []
        langs = ['fr', 'it', 'en']

        dataset_attrs = dataset.find('dataset_attributes')
        for attr in self.DATASET_ATTRIBUTES:
            term = dataset_attrs.find(attr).find('de').text
            log.debug('Create translation for %s' % term)
            if attr == 'tags':
                for lang in langs:
                    trans = dataset_attrs.find(attr).find(lang).text
                    # Tags are split and translated individually
                    split_term = self._clean_values(term.split(','))
                    split_trans = self._clean_values(trans.split(','))

                    if len(split_term) == len(split_trans):
                        for term, trans in zip(split_term, split_trans):
                            log.debug(
                                'Term (tag): %s, Translation (%s): %s'
                                % (term, lang, trans)
                            )
                            translations.append({
                                u'lang_code': lang,
                                u'term': munge_tag(term),
                                u'term_translation': munge_tag(trans)
                            })
            else:
                for lang in langs:
                    trans = dataset_attrs.find(attr).find(lang).text
                    if term != trans:
                        log.debug(
                            'Term: %s, Translation (%s): %s'
                            % (term, lang, trans)
                        )
                        translations.append({
                            u'lang_code': lang,
                            u'term': term,
                            u'term_translation': trans
                        })
        resources = dataset.findall('resource')
        for resource in resources:
            for attr in self.RESOURCE_ATTRIBUTES:
                res_attr = resource.find('resource_attributes')
                term = res_attr.find(attr).find('de').text
                log.debug('Create translation for %s' % term)
                for lang in langs:
                    trans = res_attr.find(attr).find(lang).text
                    if term != trans:
                        log.debug(
                            'Term: %s, Translation (%s): %s'
                            % (term, lang, trans)
                        )
                        translations.append({
                            u'lang_code': lang,
                            u'term': term,
                            u'term_translation': trans
                        })
        return translations
