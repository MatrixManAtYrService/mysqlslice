import pymysql

class LocalArgs:
    def __init__(self, user, password, database, socket):
        self.user = user
        self.password = password
        self.database = database
        self.socket = socket

class RemoteArgs:
    def __init__(self, host, user, password, database, cipher=None):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.cipher = cipher

# for use like so:

#     with LocalConnection(args) as conn:
#         with conn.cursor() as cursor:
#             cursor.dostuff()            # connected here

#         other_stuff()                   # orig cursor closed
#
#         with conn.cursor() as cursor:
#             cursor.dostuff()            # new cursor
#
#     other_stuff()                       # disconnected here

class Connection:
    def __init__(self, args):
        self.args = args
        self.database = args.database

    # get a cursor for this connection
    def cursor(self):
        cursor = self.connection.cursor()

        # a server I know doesn't like to have the database name in the connection string
        # so I just specify a database on cursor creation
        cursor.execute('use {};'.format(self.args.database))

        # close the cursor when we exit a 'with' block
        cursor.__exit__ = lambda self : self.close()

        return cursor

    def __exit__(self, type, value, traceback):
        self.connection.close()

class LocalConnection(Connection):
    def __enter__(self):
        self.connection = pymysql.connect(user=self.args.user,
                                          passwd=self.args.password,
                                          cursorclass=pymysql.cursors.DictCursor)

        # store this explicitly since it doesn't get populated on connection
        self.connection.db = self.args.database
        return self

class RemoteConnection(Connection):
    def __enter__(self):

        # build ssl args
        if self.args.cipher != None:
            ssl = { 'cipher' : self.args.cipher }
        else:
            ssl = None

        self.connection = pymysql.connect(host=self.args.host,
                                          user=self.args.user,
                                          passwd=self.args.password,
                                          ssl=ssl,
                                          cursorclass=pymysql.cursors.DictCursor)

        # store this explicitly since it doesn't get populated on connection
        self.connection.db = self.args.database
        return self

