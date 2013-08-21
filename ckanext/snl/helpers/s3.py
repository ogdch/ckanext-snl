from boto.s3.connection import S3Connection
from boto.s3.key import Key
from ckan.plugins.core import SingletonPlugin, implements
from ckan.plugins.interfaces import IConfigurable
from pylons import config
import os

class S3():
    def __init__(self):
        try:
            self.key = config['ckanext.snl.s3_key']
            self.token = config['ckanext.snl.s3_token']
            self.bucket_name = config['ckanext.snl.s3_bucket']
            conn = S3Connection(self.key,self.token)
            self.bucket = conn.get_bucket(self.bucket_name)
        except KeyError as e:
            raise ConfigEntryNotFoundError("'%s' not found in config" % e.message)
    
    def __repr__(self):
        return "<S3 key:%s token:%s bucket_name:%s>" % (self.key, self.token, self.bucket_name)

    def list(self, prefix=None):
        for key in self.bucket.list(prefix=prefix):
            yield key

    def list_names(self, prefix=None):
        for key in self.list(prefix):
            yield key.name.encode('utf-8')

    def download_bucket_to_dir(self, bucket_name, dir_name):
        files = []
        prefix = bucket_name + '/'
        for key in self.list(prefix):
            dump_file = os.path.join(dir_name, key.name.replace(prefix, '').encode('utf-8'))
            key.get_contents_to_filename(dump_file)
            files.append(dump_file)
        return files

    def upload_dir_to_bucket(self, bucket_name, dir_name):
        for filename in os.listdir(dir_name):
            self.upload_file_to_bucket(bucket_name, dir_name, filename)

    def upload_file_to_bucket(self, bucket_name, dir_name, filename):
        key = Key(self.bucket)
        key.key = bucket_name + '/' + filename
        key.set_contents_from_filename(os.path.join(dir_name, filename))


class ConfigEntryNotFoundError(Exception):
    pass
