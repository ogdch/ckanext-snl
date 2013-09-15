ckanext-snl
===========

Harvester for the Swiss National Library (SNL)

## Installation

Use `pip` to install this plugin. This example installs it in `/home/www-data`

```bash
source /home/www-data/pyenv/bin/activate
pip install -e git+https://github.com/ogdch/ckanext-snl.git#egg=ckanext-snl --src /home/www-data
cd /home/www-data/ckanext-snl
pip install -r pip-requirements.txt
python setup.py develop
```

Make sure to add `snl` and `snl_harvester` to `ckan.plugins` in your config file.

## Run harvester

```bash
source /home/www-data/pyenv/bin/activate
paster --plugin=ckanext-snl snl_harvester gather_consumer -c development.ini &
paster --plugin=ckanext-snl snl_harvester fetch_consumer -c development.ini &
paster --plugin=ckanext-snl snl_harvester run -c development.ini
```

Only harvest files via OAI-PMH:

```bash
source /home/www-data/pyenv/bin/activate
cd /home/www-data/pyenv/src/ckan
 
# Export the oai entries for the specified set
# This command harvests the whole dataset and uploads the resulting records.xml to S3
 paster --plugin=ckanext-snl snl export e-diss -c production.ini
  
# Resume export of the oai entries for the specified set
# This command resumes the harvesting of the "sb" set, beginning from record 106500 and it stops at record 1000000
# If you specify an upper limit the files are not uploaded to S3, but are only kept locally.
paster --plugin=ckanext-snl snl resume sb 106500 1000000 -c production.ini
```
