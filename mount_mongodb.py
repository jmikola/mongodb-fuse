import errno
import fuse
import stat
import sys

from bson.objectid import ObjectId
from pymongo.connection import Connection
from time import time

fuse.fuse_python_api = (0, 2)


class MongoStat(fuse.Stat):
    def __init__(self):
        self.st_mode = stat.S_IFDIR | 0755
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 2
        self.st_uid = 0
        self.st_gid = 0
        self.st_size = 4096
        self.st_atime = 0
        self.st_mtime = 0
        self.st_ctime = 0


class MongoFuse(fuse.Fuse):
    def __init__(self, *args, **kw):
        fuse.Fuse.__init__(self, *args, **kw)
        self.host = "localhost"
        self.db = "test"

    def getattr(self, path):
        st = MongoStat()
        pe = path.split('/')[1:]

        st.st_atime = int(time())
        st.st_mtime = st.st_atime
        st.st_ctime = st.st_atime

        if path == '/':
            return st
        elif len(pe) > 2:
            return -errno.ENOENT

        conn = Connection(host=self.host)
        db = conn[self.db]
        coll = db[pe[0]]

        if len(pe) == 2:
            doc = coll.find_one(spec_or_id=ObjectId(pe[1]))

            if doc != None:
                st.st_mode = stat.S_IFREG | 0666
                st.st_nlink = 1
                st.st_size = sys.getsizeof(doc)
            else:
                conn.disconnect()
                return -errno.ENOENT

        conn.disconnect()
        return st

    def readdir(self, path, offset):
        pe = path.split('/')[1:]
        dirents = ['.', '..']

        conn = Connection(host=self.host)
        db = conn[self.db]

        if path == '/':
            for name in db.collection_names():
                dirents.append(str(name))
        elif len(pe) == 1:
            coll = db[pe[0]]
            docs = coll.find(fields=['_id'])
            for id in [doc['_id'] for doc in docs]:
                dirents.append(str(id))

        conn.disconnect()

        for r in dirents:
            yield fuse.Direntry(r)

    def open(self, path, flags):
        return 0

    def truncate(self, path, size):
        return 0

    def utime(self, path, times):
        return 0

    def mkdir(self, path, mode):
        return 0

    def rmdir(self, path):
        return 0

    def rename(self, pathfrom, pathto):
        return 0

    def fsync(self, path, isfsyncfile):
        return 0


def main():
    usage = """
        mongodb-fuse: A FUSE wrapper for MongoDB
    """ + fuse.Fuse.fusage

    server = MongoFuse(version="%prog " + fuse.__version__, usage=usage,
        dash_s_do='setsingle')
    server.parser.add_option(mountopt="host", metavar="HOSTNAME",
        default=server.host, help="MongoDB server host [default: %default]")
    server.parser.add_option(mountopt="db", metavar="DATABASE",
        default=server.db, help="MongoDB database name [default: %default]")
    server.parse(values=server, errex=1)
    server.main()


if __name__ == '__main__':
    main()
