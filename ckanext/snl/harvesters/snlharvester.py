#n -*- coding: utf-8 -*-

import random
import os
import shutil
import tempfile
import zipfile
from pprint import pprint
from collections import defaultdict

from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action, action
from ckan.lib.helpers import json

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, HarvestObjectError
from base import OGDCHHarvesterBase

from ckanext.snl.helpers import s3

import logging
log = logging.getLogger(__name__)

class SNLHarvester(OGDCHHarvesterBase):
    '''
    The harvester for snl
    '''

    HARVEST_USER = u'harvest'

    FILES_BASE_URL = 'http://opendata-ch.s3.amazonaws.com'


    def info(self):
        return {
            'name': 'snl',
            'title': 'SNL',
            'description': 'Harvests the snl data',
            'form_config_interface': 'Text'
        }

    def gather_stage(self, harvest_job):
        log.debug('In SNLHarvester gather_stage')


    def fetch_stage(self, harvest_object):
        log.debug('In SNLHarvester fetch_stage')

    def import_stage(self, harvest_object):
        log.debug('In SNLHarvester import_stage')

