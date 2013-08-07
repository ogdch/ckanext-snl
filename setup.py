from setuptools import setup, find_packages
import sys, os

version = '0.0'

setup(
	name='ckanext-snl',
	version=version,
	description="CKAN extension of the Swiss National Library for the OGD portal of Switzerland",
	long_description="""\
	""",
	classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
	keywords='',
	author='Liip AG',
	author_email='ogd@liip.ch',
	url='http://www.liip.ch',
	license='GPL',
	packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
	namespace_packages=['ckanext', 'ckanext.snl'],
	include_package_data=True,
	zip_safe=False,
	install_requires=[
		# -*- Extra requirements: -*-
	],
	entry_points=\
	"""
    [ckan.plugins]
    snl=ckanext.snl.plugins:SNLHarvest
    snl_harvester=ckanext.snl.harvesters:SNLHarvester
    [paste.paster_command]
    snl=ckanext.snl.commands.snl:SNLCommand
    snl_harvester=ckanext.snl.commands.harvester:Harvester
	""",
)
