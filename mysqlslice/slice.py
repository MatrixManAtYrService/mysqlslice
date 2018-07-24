#! /usr/bin/env python3

from mysqlslice.cli import parse_pull_args, Prindenter, Indent

from mysqlslice.mysql import LocalConnection, LocalArgs, RemoteConnection, RemoteArgs

from mysqlslice.sync import general_sync, pull_foo, pull_missing_ids


# Different tables may require different functionality for syncing.
# Map that functionality on a table-by-table basis here.
def get_steps(cli_args, local_args, remote_connection, printer=Prindenter()):

    return {
            'foo_tokens' : lambda : pull_foo(cli_args, local_args, remote_connection, printer=printer),
            # 'foo' gets it's own sync function.
            # It pulls only a subset of `foo_ref` and then pulls only the subset of `foo_token`that is referenced by `foo_ref`.
            # It always clobbers the entire target table with the new data.

            'foo_ref'    : None,
            # This table also handled by pull_foo

            'bar' : lambda : general_sync('bar', 15, cli_args, local_args, remote_connection, printer=printer),
            # 'bar' experiences INSERTs, DELETEs, and UPDATEs, so it needs a full sync
            # gerenal_sync first calls pull_missing_id, so any newly added rows are present in the target.
            #   Then it partitions the table based on id-range and compares MD5 hashes of each partition, and
            #   then it transfers whichever partitions contained changes.
            # Based on its size, we'll sync it in batches of 15
            #   too large means we spend less time finding the changes and more time moving data
            #   too small means we spend more time finding the changes and less time moving data
            #   consider the relative value of network bandwith vs cpu time in your own case

            'baz' : lambda : pull_missing_ids('baz', cli_args, local_args, remote_connection, printer=printer),
            # this table only experiences INSERTs, so we can just sync via max(id)
            }


def main(args):
    printer = Prindenter(indent=0)

    printer('Syncing {} to localhost'.format(args.remote_host))

    # defer local connection setup
    local_args = LocalArgs(args.local_user, args.local_password, args.local_database, args.local_socket)

    # set up remote connection
    remote_args = RemoteArgs(args.remote_host, args.remote_user, args.remote_password, args.remote_database)

    if hasattr(args, 'cipher'):
        remote_args = RemoteArgs(args.remote_host, args.remote_user, args.remote_password,
                                 args.remote_database, cipher=args.cipher)
    else:
        remote_args = RemoteArgs(args.remote_host, args.remote_user, args.remote_password,
                                 args.remote_database)

    printer('connecting with:')
    printer(remote_args.__dict__)
    with RemoteConnection(remote_args) as remote_connection, Indent(printer):

        # do the sync-steps for each table in the slice
        for table_name, sync_func in get_steps(args, local_args, remote_connection, printer).items():
            printer('[Table: {}]'.format(table_name))
            with Indent(printer):
                if sync_func:
                    sync_func()
                    printer("")
                else:
                    with Indent(printer):
                        printer("skipped explicitly by slice definition")
                        printer("")

    printer('Done')

# used as entrypoint in setup.py
def pull():
    global cli_args
    cli_args = parse_pull_args('update a local stale slice from remote freshness')
    main(cli_args)

# called when this script is run directly
if __name__ == '__main__':
    pull()
