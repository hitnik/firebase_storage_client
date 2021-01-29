from firebase_admin import credentials, initialize_app, storage
from google.api_core.exceptions import NotFound
import tarfile
import os
import re


class FireBaseStorageClient:
    """
    A class to represent a Google firebase storage client.

    Attributes
    ----------
    bucket : str
        Firebase bucket e.g "my-application.appspot.com"
    folder : str
        Folder name in bucket. Default value None

    Methods
    -------
    upload_file(self, file, archive=False):
        Upload file to bucket with pre-archive of without it

    download_file(self, url, to_path, is_archive=False):
        Download file from bucket with extract or without it

    delete_file(self, url):
        Delete file from bucket
    """

    def __init__(self, cred_path, bucket, folder=None):
        """
        :param cred_path: str
                Path of Google firestone credentials.json file
        :param bucket: str
                 FireStone bucket e.g "my-application.appspot.com"
        :param folder: str
                Folder name in bucket. Default value None
        """
        cred = credentials.Certificate(cred_path)
        initialize_app(cred, {'storageBucket': bucket})
        self.bucket = storage.bucket()
        self.folder = folder

    def _archive_file(self, path):
        """
            Archive file to tar.gz archive
        :param path:
                Path of file to archive
        :return:
                tar.gz path
        """
        head, tail = os.path.split(path)
        output_filename = re.sub(r'.\w+$', '.tar.gz', tail)
        tar = tarfile.TarFile.gzopen(os.path.join(head, output_filename), mode='w', compresslevel=5)
        tar.add(path, arcname=os.path.basename(path))
        tar.close()
        return os.path.join(head, output_filename)

    def _extract(self, path):
        """
            Extract tar.gz archive into folder
        :param path: str
            tar.gz path
        :return:
            path of folder with extracted files
        """
        head, tail = os.path.split(path)
        tf = tarfile.open(path)
        if len(tf.getmembers()) == 1:
            filename = tf.getmembers()[0].name
        else:
            raise FileNotFoundError('Cant recognize name of file')
        tf.extractall(path=head)
        tf.close()
        if os.path.exists(path):
            os.remove(path)
        return os.path.join(head, filename)

    def upload_file(self, file, archive=False):
        """
            Upload file to bucket with pre-archive of without it
        :param file: str
            file path to upload
        :param archive: bool
            with archive previously or without
        :return:
            file url in bucket
        """
        tar = self._archive_file(file) if archive else file
        head, file_name = os.path.split(tar)
        if self.folder:
            url = self.folder + '/' + file_name
        else:
            url = file_name

        blob = self.bucket.blob(url)
        blob.upload_from_filename(tar)

        if os.path.exists(file):
            os.remove(file)
        if os.path.exists(tar):
            os.remove(tar)
        return url

    def download_file(self, url, to_path, extract=False):
        """
            Download file from bucket with extract or without it
        :param url: str
            URL of file in bucket
        :param to_path: str
            path to save file
        :param extract: bool
            wih extract or not
        :return:
            saved file path
        """
        blob = self.bucket.blob(url)
        if not os.path.exists(to_path):
            os.mkdir(to_path, 0o755)
        head, file = os.path.split(url)
        path = os.path.join(to_path, file)
        try:
            blob.download_to_filename(path)
        except NotFound:
            raise FileNotFoundError('File not found on server')
        extract_path = self._extract(path) if extract else path
        return extract_path

    def delete_file(self, url):
        """
            Delete file from bucket
        :param url: str
            File URL in bucket
        :return:
            None
        """
        blob = self.bucket.blob(url)
        blob.delete()



