#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import errno
import urllib
import hashlib
import json

from time import sleep

import math

import subprocess
from fuse import FUSE, FuseOSError, Operations

BLOCKSIZE = 4096
path_md5_map = {}


class Passthrough(Operations):
    def __init__(self, root):
        self.root = root
        self.host = "10.211.55.7"
        self.port = "8080"
        self.use_lock = True

    # Helpers
    # =======

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    def md5(self, fname):
        hash_md5 = hashlib.md5()
        with open(fname, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def block_level_md5(self, fname):
        final_md5 = []
        hasher = hashlib.md5()
        with open(fname, 'rb') as afile:
            buf = afile.read(BLOCKSIZE)
            buf1 = afile.read(BLOCKSIZE)
            while True:
                buf1 = buf
                buf = afile.read(BLOCKSIZE)  # buf is a lookahead
                hasher.update(buf1)  # buf1 is the current buffer
                md5 = hasher.hexdigest()
                final_md5.append(md5)  # we need a way to retrieve these by file and offset
                if (len(buf) == 0):
                    break
        afile.close()
        path_md5_map.update({fname: final_md5})

    def block_level_md5_by_offset(self, full_path, offset):
        file = open(full_path, 'rb')
        file.seek(offset)
        bytes = file.read(BLOCKSIZE)
        file.close()

        md5 = self.md5_from_bytes(bytes)

        print("bytes")
        print(bytes)
        print("md5")
        print(md5)

        return md5

    def md5_from_bytes(self, bytes):
        hasher = hashlib.md5()
        hasher.update(bytes)
        md5 = hasher.hexdigest()
        return md5

    def perform_unlock(self, lock_json, path, block_index):
        md5 = lock_json['md5']
        operation = "unlock" if self.use_lock else 'unlease'
        user_id = "1"
        resource_path = path
        lock_type = "WRITE"

        params = {
            'lock_type': lock_type,
            'operation': operation,
            'resource_path': resource_path,
            'user_id': user_id,
            'md5': md5,
            'block_index': block_index
        }

        if not self.use_lock:
            params['leaseKey'] = lock_json['leaseKey']

        url = self.build_url(params)

        print(url)
        response = urllib.urlopen(url).read()
        json_dictionary = json.loads(response)
        return json_dictionary

    def perform_lock(self, path, block_index):
        operation = "lock" if self.use_lock else 'lease'
        user_id = "1"
        resource_path = path
        lock_type = "WRITE"

        url = self.build_url({
            'lock_type': lock_type,
            'operation': operation,
            'resource_path': resource_path,
            'user_id': user_id,
            'block_index': block_index
        })

        print(url)

        response = urllib.urlopen(url).read()
        json_dictionary = json.loads(response)
        return json_dictionary

    def build_url(self, parameters):
        url = "http://" \
              + self.host + ":" \
              + self.port + "/" \
              + parameters['operation'] \
              + "?userId=" + parameters['user_id'] \
              + "&resourcePath=" + parameters['resource_path'] \
              + "&lockType=" + parameters['lock_type'] \
              + "&blockIndex=" + str(int(parameters['block_index']))

        if 'md5' in parameters:
            url += "&md5=" + parameters['md5']

        if 'leaseKey' in parameters:
            url += "&leaseKey=" + parameters['leaseKey']

        return url

    def findMD5(self, json_dictionary):
        return json_dictionary['md5']

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        full_path = self._full_path(path)
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        full_path = self._full_path(path)
        st = os.lstat(full_path)
        return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                                                        'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size',
                                                        'st_uid'))

    def readdir(self, path, fh):
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r

    def readlink(self, path):
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        return os.mkdir(self._full_path(path), mode)

    def statfs(self, path):
        full_path = self._full_path(path)
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
                                                         'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files',
                                                         'f_flag',
                                                         'f_frsize', 'f_namemax'))

    def unlink(self, path):
        return os.unlink(self._full_path(path))

    def symlink(self, name, target):
        return os.symlink(name, self._full_path(target))

    def rename(self, old, new):
        return os.rename(self._full_path(old), self._full_path(new))

    def link(self, target, name):
        return os.link(self._full_path(target), self._full_path(name))

    def utimens(self, path, times=None):
        return os.utime(self._full_path(path), times)

    # File methods
    # ============

    def open(self, path, flags):
        full_path = self._full_path(path)
        print("printing full path " + full_path)
        if not path_md5_map or not (full_path in path_md5_map):
            self.block_level_md5(full_path)
        return os.open(full_path, flags)

    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        print("printing offset")
        print(offset)
        print("making rest call")
        block_index = self.get_block_index(offset)
        stat = self.perform_lock(path, block_index)
        md5_from_server = self.findMD5(stat)
        print("md5_from_server: " + md5_from_server)
        full_path = self._full_path(path)
        md5_from_file = self.get_md5_from_file_by_offset(full_path, offset)
        print("md5_from_file" + md5_from_file)
        if self.use_lock:
            md5_from_file = self.wait_white_md5_does_not_match(full_path, md5_from_file, md5_from_server, offset)
        else:
            if md5_from_file != md5_from_server and md5_from_server != 'N/A':
                message = "The file: \n"+path+"\nIs currently being edited by another user\n editing this file could cause conflicts"
                self.send_message(message)
        print('md5FromFile: ' + md5_from_file)
        print("before some read is happening: " + path)
        os.lseek(fh, offset, os.SEEK_SET)
        print("after some read is happening: " + path)
        print(md5_from_server)

        # remove md5 so it is not reset, this is a read operation,
        # rest server will not set to empty string
        stat['md5'] = ''

        unlock_result = self.perform_unlock(stat, path, block_index)
        print(unlock_result)

        with open(full_path, "rb") as f:
            f.seek(offset, os.SEEK_SET)
            return_bytes = f.read(length)
        f.close()
        return return_bytes

    def wait_white_md5_does_not_match(self, full_path, md5_from_file, md5_from_server, offset):
        while md5_from_server != md5_from_file and md5_from_server != 'N/A':
            sleep(0.2)
            md5_from_file = self.block_level_md5_by_offset(full_path, offset)
            print('waiting: ' + md5_from_file)
        return md5_from_file

    def get_md5_from_file_by_offset(self, full_path, offset):
        md5_of_file = path_md5_map.get(full_path)
        block_index = int(self.get_block_index(offset))
        md5_from_file = md5_of_file[block_index - 1]
        return md5_from_file

    def write(self, path, buf, offset, fh):
        print("some write is happening, path: " + path)
        print("printting offset")
        print(offset)
        block_index = self.get_block_index(offset)
        stat = self.perform_lock(path,block_index)
        full_path = self._full_path(path)
        md5_from_server = self.findMD5(stat)
        block_offset = self.get_block_offset(offset)
        md5_from_file = self.get_md5_from_file_by_offset(full_path, block_offset)

        if self.use_lock:
            self.wait_white_md5_does_not_match(full_path, md5_from_file, md5_from_server, block_offset)


        # os.lseek(fh, offset, os.SEEK_SET)
        # write_return = os.write(fh, buf)

        write_flags = 33793
        file_descriptor = os.open(full_path, write_flags)
        os.lseek(file_descriptor,offset,os.SEEK_SET)
        write_return = os.write(file_descriptor,buf)
        os.close(file_descriptor)

        # os.lseek(fh, offset, os.SEEK_SET)
        # bye = os.read(fh,BLOCKSIZE)
        print("after the write is performed: " + path)
        full_path = self._full_path(path)

        block_offset = self.get_block_offset(offset)
        md5FromFile = self.block_level_md5_by_offset(full_path, block_offset)  # set the md5 value

        stat['md5'] = md5FromFile

        stat = self.perform_unlock(stat, path, block_index)  # md5from file is none here causing issues with concatinating string
        # /*calculate new md5*/
        # /*release the lock with new md5*/
        return write_return

    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        return os.fsync(fh)

    def release(self, path, fh):
        return os.close(fh)

    def fsync(self, path, fdatasync, fh):
        return self.flush(path, fh)

    def get_block_offset(self, offset):
        block_index = self.get_block_index(offset)
        block_offset = block_index * BLOCKSIZE
        return int(block_offset)

    def get_block_index(self, offset):
        block_index = math.floor(offset / BLOCKSIZE)
        return block_index

    def send_message(self,message):
        subprocess.Popen(['notify-send', message])
        return


def main(mountpoint, root):
    FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True)


if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
