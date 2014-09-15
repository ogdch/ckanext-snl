from boto.s3.connection import S3Connection
from boto.s3.key import Key
from pylons import config
import os
import time
import logging
log = logging.getLogger(__name__)


class S3():
    def __init__(self):
        try:
            self.key = config['ckanext.snl.s3_key']
            self.token = config['ckanext.snl.s3_token']
            self.bucket_name = config['ckanext.snl.s3_bucket']
            conn = S3Connection(self.key, self.token)
            self.bucket = conn.get_bucket(self.bucket_name)
        except KeyError as e:
            raise ConfigEntryNotFoundError(
                "'%s' not found in config"
                % e.message
            )

    def __repr__(self):
        return (
            "<S3 key:%s token:%s bucket_name:%s>"
            % (self.key, self.token, self.bucket_name)
        )

    def list(self, prefix=None):
        for key in self.bucket.list(prefix=prefix):
            yield key

    def list_names(self, prefix=None):
        for key in self.list(prefix):
            yield key.name.encode('utf-8')

    def download_bucket_to_dir(self, prefix, dir_name, ignore=None):
        files = []
        for key in self.list(prefix):
            filename = key.name.replace(prefix, '').encode('utf-8')
            if (ignore is not None and filename not in ignore):
                dump_file = os.path.join(dir_name, filename)
                key.get_contents_to_filename(dump_file)
                files.append(dump_file)
        return files

    def get_url_of_file(self, bucket_name, filename):
        bucket_path = bucket_name + '/' + filename
        key = self.bucket.get_key(bucket_path)
        return key.generate_url(0, query_auth=False, force_http=True)

    def get_size_of_file(self, bucket_name, filename):
        bucket_path = bucket_name + '/' + filename
        key = self.bucket.get_key(bucket_path)
        return self.bucket.lookup(key).size

    def upload_dir_to_bucket(self, bucket_name, dir_name):
        for filename in os.listdir(dir_name):
            self.upload_file_to_bucket(bucket_name, dir_name, filename)

    def upload_file_to_bucket(self, bucket_name, dir_name, filename):
        key = Key(self.bucket)
        key.key = bucket_name + '/' + filename

        # try 3 times to upload the file to S3
        # sometimes it runs in some strange timeouts
        max_tries = 3
        for i in range(max_tries):
            try:
                key.set_contents_from_filename(
                    os.path.join(dir_name, filename),
                    cb=percent_cb
                )
                break
            except Exception, e:
                if (i == max_tries - 1):
                    raise
                log.exception(e)
                time.sleep(30)
                continue

        # Copy the key onto itself, preserving the
        # ACL but changing the content-type
        key.copy(
            key.bucket,
            key.name,
            preserve_acl=True,
            metadata={
                'Content-Type': 'binary/octet-stream',
                'Content-Disposition': 'attachment; filename="%s"' % filename
            }
        )


def percent_cb(complete, total):
    log.debug('%i/%i' % (complete, total))


class ConfigEntryNotFoundError(Exception):
    pass
