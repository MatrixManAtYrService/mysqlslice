#! /usr/bin/env python3
from bslice.cli import parse_pull_args, Prindenter, Indent, mysqldump_schema_nofk_remote, mysqlload_local, show_do_query
from bslice.mysql import LocalConnection, LocalArgs, RemoteConnection, RemoteArgs

# global to this module, will be populated when main() is called
# contains cli args
cli_args = None

def pull_schema(local_args, remote_connection, printer=Prindenter()):

    global cli_args
    target_db = local_args.database

    with LocalConnection(local_args) as local_connection:
        with local_connection.cursor() as cursor:
            show_tables = 'show tables;'
            result = show_do_query(cursor, show_tables, printer=printer)
            table_ct = len(result)

    if table_ct > 0:

        printer("{} is a nonempty local database. "
                "If you want me to create a new database in its place, you'll have to drop and create it yourself.".format(target_db))
                # if you'd rather I nuke it for you, you're trusting me too much

    else:

        tmp_file = 'schema_nofk.sql'

        # dump schema to a file
        mysqldump_schema_nofk_remote(cli_args, tmp_file, printer=printer)

        # load from a file
        mysqlload_local(cli_args, tmp_file, printer=printer)

def main(args):
    printer = Prindenter(indent=0)

    printer('Pulling schema from {} to localhost'.format(args.p801_host))

    # defer local connection setup
    local_args = LocalArgs(args.local_user, args.local_password, args.local_database)

    # set up remote connection
    remote_args = RemoteArgs(args.p801_host, args.p801_user, args.p801_password, args.p801_database, 'DHE-RSA-AES256-SHA')
    with RemoteConnection(remote_args) as remote_connection, Indent(printer):
        pull_schema(local_args, remote_connection, printer=printer)

    printer('Done')

# used as entrypoint in setup.py
def pull():
    global cli_args
    cli_args = parse_pull_args('start with an empty database and replace it with the frail empty shell of a remote database (schema only, no foreign keys)')
    main(cli_args)

# called when this script is run directly
if __name__ == '__main__':
    pull()
