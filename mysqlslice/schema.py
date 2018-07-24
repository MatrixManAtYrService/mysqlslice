#! /usr/bin/env python3
from mysqlslice.cli import parse_pull_args, Prindenter, Indent, mysqldump_schema_nofk_remote, mysqlload_local, show_do_query
from mysqlslice.mysql import LocalConnection, LocalArgs, RemoteConnection, RemoteArgs

def pull_schema(cli_args, local_args, remote_connection, printer=Prindenter()):

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

    printer('Pulling schema from {} to localhost'.format(args.remote_host))

    # defer local connection setup
    local_args = LocalArgs(args.local_user, args.local_password, args.local_database, args.local_socket)

    # set up remote connection
    remote_args = RemoteArgs(args.remote_host, args.remote_user, args.remote_password, args.remote_database, args.cipher)
    with RemoteConnection(remote_args) as remote_connection, Indent(printer):
        pull_schema(args, local_args, remote_connection, printer=printer)

    printer('Done')

# used as entrypoint in setup.py
def pull():
    cli_args = parse_pull_args('start with an empty database and replace it with the frail empty shell of a remote database (schema only, no foreign keys)')
    main(cli_args)

# called when this script is run directly
if __name__ == '__main__':
    pull()
