from __future__ import unicode_literals
# coding: utf-8

# PYTHON IMPORTS
import os
import shutil

# DJANGO IMPORTS
from django.core.files.move import file_move_safe
from django.core.files.base import ContentFile


class StorageMixin(object):
    """
    Adds some useful methods to the Storage class.
    """

    def isdir(self, name):
        """
        Returns true if name exists and is a directory.
        """
        raise NotImplementedError()

    def isfile(self, name):
        """
        Returns true if name exists and is a regular file.
        """
        raise NotImplementedError()

    def move(self, old_file_name, new_file_name, allow_overwrite=False):
        """
        Moves safely a file from one location to another.

        If allow_ovewrite==False and new_file_name exists, raises an exception.
        """
        raise NotImplementedError()

    def makedirs(self, name):
        """
        Creates all missing directories specified by name. Analogue to os.mkdirs().
        """
        raise NotImplementedError()

    def rmtree(self, name):
        """
        Deletes a directory and everything it contains. Analogue to shutil.rmtree().
        """
        raise NotImplementedError()


class FileSystemStorageMixin(StorageMixin):

    def isdir(self, name):
        return os.path.isdir(self.path(name))

    def isfile(self, name):
        return os.path.isfile(self.path(name))

    def move(self, old_file_name, new_file_name, allow_overwrite=False):
        file_move_safe(self.path(old_file_name), self.path(new_file_name), allow_overwrite=True)

    def makedirs(self, name):
        os.makedirs(self.path(name))

    def rmtree(self, name):
        shutil.rmtree(self.path(name))


class S3BotoStorageMixin(StorageMixin):

    def isfile(self, name):
        return (os.path.basename(name) != '.folder') and self._exists(name)

    def isdir(self, name):
        if not name:  # Empty name is a directory
            return True

        if self.isfile(name):
            return False

        return self._exists(os.path.join(name, '.folder'))

    def move(self, old_file_name, new_file_name, allow_overwrite=False):
        if self.exists(new_file_name):
            if not allow_overwrite:
                raise "The destination file '%s' exists and allow_overwrite is False" % new_file_name

            if self.isfile(new_file_name):
                self.delete(new_file_name)
            elif self.isdir(new_file_name):
                self.rmtree(new_file_name)

        if self.isfile(old_file_name):
            old_key_name = self._encode_name(self._normalize_name(self._clean_name(old_file_name)))
            new_key_name = self._encode_name(self._normalize_name(self._clean_name(new_file_name)))

            k = self.bucket.copy_key(new_key_name, self.bucket.name, old_key_name)
            if not k:
                raise "Couldn't copy '%s' to '%s'" % (old_file_name, new_file_name)
            self.delete(old_file_name)
        elif self.isdir(old_file_name):
            oldname = self._normalize_name(self._clean_name(old_file_name))
            newname = self._normalize_name(self._clean_name(new_file_name))

            base_parts = len(oldname.split("/"))
            for item in self.bucket.list(self._encode_name(oldname)):
                parts = item.name.split("/")
                parts = parts[base_parts:]
                new_item_name = os.path.join(newname, *parts)
                k = self.bucket.copy_key(self._encode_name(new_item_name), self.bucket.name, self._encode_name(item.name))
                if not k:
                    raise "Couldn't copy '%s' to '%s'" % (item.name, new_item_name)
                self.bucket.delete_key(self._encode_name(item.name))

    def makedirs(self, name):
        if not name:
            return

        dirname, _ = os.path.split(name)
        self.makedirs(dirname)
        if self.isfile(name):
            raise FileExistsError("File exists: '%s'" % name)

        if not self.isdir(name):
            self.save(os.path.join(name, ".folder"), ContentFile(""))

    def rmtree(self, name):
        name = self._normalize_name(self._clean_name(name))
        dirlist = self.bucket.list(self._encode_name(name))
        for item in dirlist:
            item.delete()


GoogleStorageMixin = S3BotoStorageMixin
OSSBotoStorageMixin = S3BotoStorageMixin
