from boto.s3.connection import S3Connection
from boto.s3.key import Key
from filechunkio import FileChunkIO
from multiprocessing import Pool
from pylons import config
import math
import os
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
                log.debug('Dump file %s to %s' % (key.key, dump_file))
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

    def upload_file_to_bucket(self, bucket_name, dir_name, filename,
                              parallel_processes=4):
        key = Key(self.bucket)
        key.key = bucket_name + '/' + filename

        default_chunk_size = 5242880  # ~5MB
        source_path = os.path.join(dir_name, filename)
        source_size = os.stat(source_path).st_size
        bytes_per_chunk = max(
            int(math.sqrt(default_chunk_size) * math.sqrt(source_size)),
            default_chunk_size
        )
        chunk_amount = int(math.ceil(source_size / float(bytes_per_chunk)))

        mp = self.bucket.initiate_multipart_upload(key.key)
        pool = Pool(processes=parallel_processes)
        log.debug('Start upload of %s' % source_path)
        for i in range(chunk_amount):
            offset = i * bytes_per_chunk
            remaining_bytes = source_size - offset
            bytes = min([bytes_per_chunk, remaining_bytes])
            part_num = i + 1
            pool.apply_async(
                self._upload_part,
                [
                    self.key,
                    self.token,
                    self.bucket_name,
                    mp.id,
                    part_num,
                    source_path,
                    offset,
                    bytes
                ]
            )
        pool.close()
        pool.join()

        if len(mp.get_all_parts()) == chunk_amount:
            mp.complete_upload()
            log.info('Upload of %s completed' % source_path)
            # Copy the key onto itself, preserving the
            # ACL but changing the content-type
            key.copy(
                key.bucket,
                key.name,
                preserve_acl=True,
                metadata={
                    'Content-Type': 'binary/octet-stream',
                    'Content-Disposition': 'attachment; filename="%s"' %
                    filename
                }
            )
        else:
            log.error('Upload of %s failed, cancel' % source_path)
            mp.cancel_upload()

    # inspired by
    # www.topfstedt.de/python-parallel-s3-multipart-upload-with-retries.html
    def _upload_part(self, aws_key, aws_secret, bucket_name, multipart_id,
                     part_num, source_path, offset, bytes,
                     amount_of_retries=10):
        """
        Uploads a part with retries.
        """
        def _upload(retries_left=amount_of_retries):
            try:
                log.info(
                    'Start uploading part #%d of %s' % (part_num, source_path)
                )
                conn = S3Connection(aws_key, aws_secret)
                bucket = conn.get_bucket(bucket_name)
                for mp in bucket.get_all_multipart_uploads():
                    if mp.id == multipart_id:
                        with FileChunkIO(source_path, 'r', offset=offset,
                                         bytes=bytes) as fp:
                            mp.upload_part_from_file(fp=fp, part_num=part_num)
                        break
            except Exception, e:
                if retries_left:
                    _upload(retries_left=retries_left - 1)
                else:
                    log.debug('Failed uploading part #%d' % part_num)
                    log.exception(e)
                    raise e
            else:
                log.info('Uploaded part #%d' % part_num)

        _upload()


class ConfigEntryNotFoundError(Exception):
    pass
