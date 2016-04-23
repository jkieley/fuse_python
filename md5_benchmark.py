#!/usr/bin/env python

from __future__ import with_statement

import os
import sys
import errno
import urllib
import hashlib
import json
import math
import subprocess
from time import sleep
from fuse import FUSE, FuseOSError, Operations


class Passthrough(Operations):

    def main(mountpoint, root):
        FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True)

    if __name__ == '__main__':
        main(sys.argv[2], sys.argv[1])
