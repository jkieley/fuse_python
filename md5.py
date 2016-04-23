import hashlib


class Md5Helper():

    def __init__(self):
        self.BLOCKSIZE = 4096

    def md5_entire_file(self, full_path):
        hash_md5 = hashlib.md5()
        with open(full_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def md5_block(self, full_path):
        offset = 0
        file_handle = open(full_path, 'rb')
        file_handle.seek(offset)
        read_bytes = file_handle.read(self.BLOCKSIZE)
        file_handle.close()

        md5 = self.md5_from_bytes(read_bytes)
        return md5

    def md5_from_bytes(self, bytes):
        hasher = hashlib.md5()
        hasher.update(bytes)
        md5 = hasher.hexdigest()
        return md5
