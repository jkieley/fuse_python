#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import errno
import urllib
import hashlib

from time import sleep
from fuse import FUSE, FuseOSError, Operations

BLOCKSIZE = 4096
path_md5_map = {}


class Passthrough(Operations):
    def __init__(self, root):
        self.root = root
        self.host = "10.211.55.7"
        self.port = "8080"

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
                buf = afile.read(BLOCKSIZE)
                hasher.update(buf1)
                md5 = hasher.hexdigest()
                final_md5.append(md5)
                if (len(buf) == 0):
                    break
        afile.close()
        path_md5_map.update({fname: final_md5})

    def restClientUser(self, path, num, md5):
        if (num == 0):
            str = "http://" + self.host + ":" + self.port + "/lock?userId=1&resourcePath=abcde&lockType=WRITE"
            print(str)
            res = urllib.urlopen(str).read()
            print(res)
            # print(md5('asdf.txt'))

        else:
            res = urllib.urlopen(
                "http://" + self.host + ":" + self.port + "/unlock?userId=1&resourcePath=abcde&lockType=WRITE&md5=" + md5).read()
            # print(md5('asdf.txt'))

        return res

    def findMD5(self, string):
        return string.replace('{', '').replace('}', '').split(',')[3].split(':')[1].replace('"', '').replace(' ', '')

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
        stat = self.restClientUser(path, 0, 100)
        md5 = self.findMD5(stat)
        print("md5: " + md5)
        if md5 is not False:
            full_path = self._full_path(path)
            md5_of_file = path_md5_map.get(full_path)
            buf_count = 0
            while buf_count <= offset:
                buf_count += BLOCKSIZE
            buf_count /= BLOCKSIZE
            md5_from_file = md5_of_file[buf_count - 1]
            print(md5_from_file)
            while md5 != md5_from_file and md5 != 'N/A':
                sleep(0.2)
                md5_from_file = md5_of_file[buf_count - 1]
                print('waiting: ' + md5_from_file)
            print('md5FromFile: ' + md5_from_file)
            print("before some read is happening: " + path)
            os.lseek(fh, offset, os.SEEK_SET)
            print("after some read is happening: " + path)
            print(md5)
            print(self.restClientUser(path, 1, md5))

            with open(full_path, "rb") as f:
                f.seek(offset, os.SEEK_SET)
                return_bytes = f.read(length)
            f.close()
            return return_bytes

    def write(self, path, buf, offset, fh):
        print("some write is happening, path: " + path)
        print("printting offset")
        print(offset)
        stat = self.restClientUser(path, 0, 100)
        os.lseek(fh, offset, os.SEEK_SET)
        write_return = os.write(fh, buf)
        print("after the write is performed: " + path)
        prefix = '/home/alekhya/Desktop/AMS/fuse_python/dir_x'
        full_path = self._full_path(path)
        md5FromFile = self.block_level_md5(full_path, offset)  # set the md5 value

        stat = self.restClientUser(path, 1, md5FromFile)
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


def main(mountpoint, root):
    FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True)


if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
