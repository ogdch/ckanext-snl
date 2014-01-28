import xlrd
import logging
log = logging.getLogger(__name__)

from ckanext.harvest.harvesters.base import munge_tag


class MetaDataParser(object):

    # Mapping of the rows in the xls to dict keys
    ROW_TYPES = (
        'ckan_entity',
        'ckan_attribute',
        'ckan_description',
        'source',
        'value_de',
        'value_fr',
        'value_it',
        'value_en',
    )

    # Only these attributes will be imported into dataset
    DATASET_ATTRIBUTES = (
        'id',
        'name',
        'title',
        'url',
        'notes',
        'author',
        'maintainer',
        'maintainer_email',
        'license_id',
        'license_url',
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
        self.workbook = xlrd.open_workbook(file_name)

    def parse_sheet(self, sheet_name):
        '''
        Parse dataset and resources of one sheet and return them as dict
        '''
        rows = self._get_row_dict_array(sheet_name)

        metadata = self._build_dataset_dict(rows)

        if 'name' in metadata:
            metadata['name'] = munge_tag(metadata['name'])
            metadata['resources'] = self._build_resources_list(rows)
            metadata = self._handle_license(metadata)
            metadata['translations'] = self._build_term_translations(rows)

        return metadata

    def _get_row_dict_array(self, sheet_name):
        '''
        Returns all rows from a sheet as dict
        '''
        worksheet = self.workbook.sheet_by_name(sheet_name)

        rows = []
        for row_num in range(1, worksheet.nrows):
            row_values = worksheet.row_values(row_num)
            row_values_clean = self._clean_values(row_values)
            row_data = dict(zip(self.ROW_TYPES, row_values_clean))
            rows.append(row_data)

        return rows

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

    def _build_dataset_dict(self, rows):
        '''
        Creates a dict from all dataset rows with all values in German
        '''
        dataset = {}

        for row in rows:
            if row.get('ckan_entity') == 'Dataset' and \
               row.get('ckan_attribute') in self.DATASET_ATTRIBUTES:
                dataset[row.get('ckan_attribute')] = row.get('value_de')

        return dataset

    def _build_resources_list(self, rows):
        '''
        Create a list from all resources in the rows
        '''
        current = {}
        resources = [current, ]
        for row in rows:
            if row.get('ckan_entity') == 'Resource' and \
               row.get('ckan_attribute') in self.RESOURCE_ATTRIBUTES:
                attr = row.get('ckan_attribute')
                if attr in current:
                    # When attributes is already present in current resource,
                    # this must be a new one
                    current = {}
                    resources.append(current)
                current[attr] = row.get('value_de')

        return resources

    def _handle_license(self, metadata):
        if (metadata['license_id'] == 'CCO'):
            metadata['license_id'] = 'cc-zero'

        return metadata

    def _build_term_translations(self, rows):
        """
        Generate meaningful term translations for all translated values
        """
        translations = []
        for row in rows:
            entity = row.get('ckan_entity')
            key = row.get('ckan_attribute')
            # ignore translations for unuused fields
            if (entity == 'Dataset' and key not in self.DATASET_ATTRIBUTES):
                log.debug('Omit dataset attribute %s' % (key, 'n/a'))
                continue
            if entity == 'Resource' and key not in self.RESOURCE_ATTRIBUTES:
                log.debug('Omit resource attribute %s' % (key, 'n/a'))
                continue

            log.debug('Create translation for %s' % key)
            values = dict(((lang, row.get('value_%s' % lang))
                          for lang in ('de', 'fr', 'it', 'en')))
            for lang, trans in values.items():
                term = values.get('de')

                # Skip german, empty and values that are not not translated
                if lang != 'de' and term and trans and term != trans:
                    if key == 'tags':
                        # Tags are splitted and translated each
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
                            log.error(
                                'Tag count mismatch: %s (de), %s (%s)'
                                % (len(split_term), len(split_trans), lang)
                            )
                    else:
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
