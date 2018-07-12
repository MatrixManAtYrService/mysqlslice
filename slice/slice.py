#! /usr/bin/env python3

# different tables may require different functionality for syncing
# e.g. an insert-only table can rely on the 'id' column exclusively
# while others may need to keep track of modified_time or somesuch
#
# map that functionality on a table-by-table basis here
def get_steps(local_args, remote_connection, printer=Prindenter()):

    return {
            # this table gets it's own sync function
            'foo_tokens' : lambda : pull_foo(local_args, remote_connection, printer=printer),

            # this table also handled by pull_foo
            'foo_ref'    : None, 
            
#            # this table experiences modifications and deletions, so it needs a full sync
#            'bar' : lambda : full_sync(local_args, remote_connection, printer=printer),

            # this table gets new rows only, so we can just sync via max(id)
            'baz' : lambda : pull_missing_ids(local_args, remote_connection, printer=printer),
            }

from bslice.cli import parse_pull_args, Prindenter, Indent, mysqldump_data_remote_batches,\
                       mysqldump_data_remote, mysqlload_local, show_do_query

from bslice.mysql import LocalConnection, LocalArgs, RemoteConnection, RemoteArgs

# global to this module, will be populated when main() is called
# contains cli args
cli_args = None

# On a server I know, remote connections get axed if they take too long
# pull this many rows per connection to fly under the radar
batch_rows = 50 # this is artificially low, you'll probably want to increase it
# I use 100000

# this logic assumes that only certain rows in foo_ref and foo_tokens actually need to be synced
def pull_foo(local_args, remote_connection, printer=Prindenter()):

    # grab only the foo token indices that are relevant
    with remote_connection.cursor() as remote_cursor:

        get_ids = '''select name, foo_token_id from foo_ref where name like 'relevant%';'''

        result = show_do_query(remote_cursor, get_ids, printer=printer)
        foo_token_ids =  ', '.join([str(x['foo_token_id']) for x in result])
        foo_ref_ids =  ', '.join([str(x['id']) for x in result])

    # dump just those rows
    mysqldump_data_remote(cli_args, 'foo_ref', 'id in ({});'.format(foo_ref_ids), printer=printer)
    mysqldump_remote(cli_args, 'foo_tokens', 'id in ({});'.format(foo_token_ids), printer=printer)

    # clear old rows
    with LocalConnection(local_args) as local_connection:
        with local_connection.cursor() as cursor:

            show_do_query(cursor, 'truncate foo_ref;', printer=printer)
            show_do_query(cursor, 'truncate foo_tokens;', printer=printer)

    # load new rows
    mysqlload_local(cli_args, 'foo_ref', printer=printer)
    mysqlload_local(cli_args, 'foo_tokens', printer=printer)

# this works for tables that only experience INSERTs, it just checks on max(id)
# and syncs the deficit
def pull_missing_ids(table_name, local_args, remote_connection, printer=Prindenter()):

    global cli_args
    target = 'max(id)';
    get_max_id = 'select {} from {};'.format(target, table_name)

    # main() doesn't pass-in a local connection because if the connection is held open
    # then mysqlload_local (below) can't make its own separate connection
    with LocalConnection(local_args) as local_connection:
        with local_connection.cursor() as cursor:
            result = show_do_query(cursor, get_max_id, printer=printer)
            begin = result[0][target]

    with remote_connection.cursor() as cursor:
        result = show_do_query(cursor, get_max_id, printer=printer)
        end = result[0][target]

    if end != begin:

        # dump to a file
        if begin == None:
            # if the target table is empty, dump everything
            mysqldump_data_remote_batches(cli_args,
                                          table_name,
                                          batch_rows,
                                          end,
                                          min_id=begin,
                                          printer=printer)
        else:
            # otherwise, dump just the rows that aren't in the target
            mysqldump_data_remote_batches(cli_args,
                                          table_name,
                                          batch_rows,    # batch size
                                          end,           # max id
                                          min_id=begin,
                                          condition='id > {}'.format(begin),
                                          printer=printer)

        # load from a file
        mysqlload_local(cli_args, table_name, printer=printer)

    else:
        printer("nothing to do")

def main(args):
    printer = Prindenter(indent=0)

    printer('Syncing {} to localhost'.format(args.remote_host))

    # defer local connection setup
    local_args = LocalArgs(args.local_user, args.local_password, args.local_database)

    # set up remote connection
    remote_args = RemoteArgs(args.remote_host, args.remote_user, args.remote_password, args.remote_database, 'DHE-RSA-AES256-SHA')
    with RemoteConnection(remote_args) as remote_connection, Indent(printer):

        # do the sync-steps for each table in the slice
        for table_name, sync_func in get_steps(local_args, remote_connection, printer).items():
            printer('[Table: {}]'.format(table_name))
            with Indent(printer):
                if sync_func:
                    sync_func()

    printer('Done')

# used as entrypoint in setup.py
def pull():
    global cli_args
    cli_args = parse_pull_args('update a local stale slice from remote freshness')
    main(cli_args)

# called when this script is run directly
if __name__ == '__main__':
    pull()
