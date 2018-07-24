# MysqlSlice

This is a proof of concept / project template.  The idea is that since general-purpose database sync tools don't know your data, they can't make sync optimizations that you can.  It provides a framework for specifying which sync strategies should be run for which tables.  It includes three sync strategies, which are mapped to tables in [mysqlslice](mysqlslice/slice.py)  Here is an excerpt:

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

If you're considering using this, you might want to first check [here](https://www.percona.com/doc/percona-toolkit/LATEST/pt-table-sync.html).  If that tool will work for you, then you should use it instead.

## Setup

### Prerequisite packages

    # you might need this for pymysql
    apt install libffi-dev libssl-dev

### Option: System Install

    python3 setup.py install

### Option: Venv Install

    python3 -m venv .venv
    source .venv/bin/activate
    pip install --upgrade pip setuptools
    python setup.py develop

## Usage

mysqlslice assumes that the sync-to database is accessable on the local system (it uses a socket found on the local filesystem to connect).  The sync-from database may also be local, but an IP address is used to connect, so it should be easy to modify mysqlslice so that this database is remote.

After installation, you will have two new commands: `pull_slice` and `pull_schema`. Each takes the same set of parameters:

    usage: pull_<slice|schema> [-h] [--local-user LOCAL_USER]
                               [--local-password LOCAL_PASSWORD]
                               [--local-database LOCAL_DATABASE]
                               [--local-socket LOCAL_SOCKET] [-u REMOTE_USER]
                               [-p REMOTE_PASSWORD] [-o REMOTE_HOST] [-d REMOTE_DATABASE]
                               [-c CIPHER]

When you specialize [slice.py](mysqlslice/slice.py) to match your data, you might also consider specializing [cli.py](mysql/cli.py) so that the default parametrs are appropriate for your use case.  Otherwise, you can just provide everything at the cli.

### pull_slice

`pull_slice` expects the local and remote databases to have identical schemas.  No checks are made here--that's up to you.  Each table listed in [slice.py](mysqlslice/slice.py) will be synced according to its specified functionality.

### pull_schema

`pull_schema` expects the local database to exist, but be empty.  It will then pull the schema from the remote database, strip the foreign keys out of it, and put it in the local one.

## Tests

See [test.sh](test.sh) for a test workflow.  Unless your local mysql user is 'root' and the password is 'test', you'll need to modify references to `pull_slice` and `pull_schema` to include the proper credentials.
