import errno
import fuse
import stat
import sys

from bson.objectid import ObjectId
from contextlib import contextmanager
from pymongo.connection import Connection
from time import time

fuse.fuse_python_api = (0, 2)

T_DB, T_COLL, T_ID, T_PROP = range(4)


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

    def __parse_path(self, path):
        """Returns a tuple of type, collection, object ID and property path"""
        pe = path.strip('/').split('/', 2)

        pathType, collName, objectId, propPath = (T_DB, None, None, None)

        if '' != pe[0]:
            pathType = T_COLL
            collName = pe[0]

        if 2 <= len(pe):
            pathType = T_ID
            objectId = ObjectId(pe[1])

        if 3 == len(pe):
            pathType = T_PROP
            propPath = pe[2].replace('/', '.')

        return (pathType, collName, objectId, propPath)

    def getattr(self, path):
        pathType, collName, objectId, propPath = self.__parse_path(path)
        
        st = MongoStat()
        st.st_atime = int(time())
        st.st_mtime = st.st_atime
        st.st_ctime = st.st_atime

        if T_DB == pathType or T_COLL == pathType:
            return st

        with open_db(self.host, self.db) as db:
            if T_PROP == pathType:
                doc = db[collName].find_one(spec={
                    "_id": objectId,
                    propPath: {"$exists": True},
                    "fields": [propPath]
                })
            else:
                doc = db[collName].find_one(spec_or_id=objectId)

            if (doc != None):
                st.st_mode = stat.S_IFREG | 0666
                st.st_nlink = 1
                st.st_size = sys.getsizeof(doc)
                return st
            else:
                return -errno.ENOENT

    def readdir(self, path, offset):
        pathType, collName, objectId, propPath = self.__parse_path(path)
        dirents = ['.', '..']

        with open_db(host=self.host, db=self.db) as db:
            if T_DB == pathType:
                for name in db.collection_names():
                    dirents.append(str(name))
            elif T_COLL == pathType:
                docs = db[collName].find(fields=['_id'])
                for id in [doc['_id'] for doc in docs]:
                    dirents.append(str(id))
            elif T_ID == pathType:
                doc = db[collName].find_one(spec_or_id=objectId)
                for key, value in doc.items():
                    dirents.append(str(key))

        for dirent in dirents:
            yield fuse.Direntry(dirent)

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


@contextmanager
def open_db(host, db):
    conn = Connection(host)
    try:
        yield conn[db]
    finally:
        conn.disconnect()


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
