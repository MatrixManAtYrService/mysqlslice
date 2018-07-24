from mysqlslice.math import make_intervals
from collections import namedtuple
from copy import deepcopy
from math import floor

from mysqlslice.cli import Prindenter, Indent, mysqldump_data_remote_batches, mysqldump_data_remote, mysqlload_local, show_do_query
from mysqlslice.mysql import LocalConnection, LocalArgs, RemoteConnection, RemoteArgs

# On a server I know, remote connections get axed if they take too long
# pull this many rows per connection to fly under the radar
batch_rows = 100000

# this logic assumes that only certain rows in foo_ref and foo_tokens actually need to be synced
def pull_foo(cli_args, local_args, remote_connection, printer=Prindenter()):

    # grab only the foo token indices that are relevant
    with remote_connection.cursor() as remote_cursor:

        get_ids = '''select id, foo_token_id from foo_ref where name like 'relevant%';'''

        result = show_do_query(remote_cursor, get_ids, printer=printer)
        foo_token_ids =  ', '.join([str(x['foo_token_id']) for x in result])
        foo_ref_ids =  ', '.join([str(x['id']) for x in result])

    # dump just those rows
    mysqldump_data_remote(cli_args, 'foo_ref', 'id in ({});'.format(foo_ref_ids), printer=printer)
    mysqldump_data_remote(cli_args, 'foo_tokens', 'id in ({});'.format(foo_token_ids), printer=printer)

    # clear old rows
    with LocalConnection(local_args) as local_connection:
        with local_connection.cursor() as cursor:

            show_do_query(cursor, 'truncate foo_ref;', printer=printer)
            show_do_query(cursor, 'truncate foo_tokens;', printer=printer)

    # load new rows
    mysqlload_local(cli_args, 'foo_ref', printer=printer)
    mysqlload_local(cli_args, 'foo_tokens', printer=printer)

    printer("foo_tokens and foo_ref are up to date where it matters")

# not all columns can be concatenated (i.e. NULL)
# this gets the list of columns and figures out how to make them concatenatabale
def examine_columns(cursor, table_name, printer=Prindenter()):

    printer("[Examining Columns on {}.{}]".format(cursor.connection.db, table_name))
    with Indent(printer):
        result = show_do_query(cursor,
                """
                SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_TYPE, COLLATION_NAME
                FROM information_schema.columns
                WHERE table_schema='{}'
                AND table_name='{}';
                """.format(cursor.connection.db, table_name),
                printer=printer)

        column_conversions= []

        for column in result:

            # make the column representation concatenate-friendly
            converted = column['COLUMN_NAME']

            if column['IS_NULLABLE'] == 'YES':
                converted = "IFNULL({}, 'NULL')".format(converted)

            if column['COLLATION_NAME'] and column['COLLATION_NAME'] not in ['NULL', 'utf8_general_ci']:
                converted = "BINARY {}".format(converted)

            # your data may deviate in new and exciting ways
            # handle them here ...

            with Indent(printer):
                printer(converted)
            column_conversions.append(converted)

        return column_conversions


def md5_row_range(cursor, table_name, column_conversions, interval, id_col='id', printer=Prindenter()):

    printer("[Fingerprinting " + cursor.connection.db
             + ".{table_name} across rows where {id_col} in {interval}]".format(**vars()))
    with Indent(printer):

        # concat-friendly conversions for the target table
        converted_columns_str = ",".join(column_conversions)

        # hash the row-range
        start = interval.start
        end = interval.end

        condition = "{id_col} >= {start} AND {id_col} < {end}".format(**vars())

        result = show_do_query(cursor,
                """
                SELECT MD5(GROUP_CONCAT(row_fingerprints)) AS range_fingerprint from
                    (SELECT MD5(CONCAT({})) as row_fingerprints
                    FROM {}
                    WHERE {}
                    ORDER BY {}) as r;
                """.format(converted_columns_str,
                           table_name,
                           condition,
                           id_col),
                printer=printer)

        return result[0]['range_fingerprint']

# row sync relies on group_concat, which silently truncates the output once it reaches
# group_concat_max_len bytes long.
# Interrogate the target server to see how many rows we can get away with.
def get_max_md5_rows(cursor, try_set=1000000 * 33, printer=Prindenter()):
    # 32 bytes for the md5 plus 1 for the comma times a million rows

    # limiting it here because I'd prefer too many small queries over a few monsters
    # that ties up the server with no gaps.  This may be unnecessarily cautious, go bigger at your own risk.

    # hell, this is all at your own risk

    printer("[How many rows is {} willing to hash at a time?]".format(cursor.connection.host))
    with Indent(printer):

        # try to ask for enough space for 1 million rows at a time
        printer("Asking for lots lof space...")
        result = show_do_query(cursor, "set session group_concat_max_len = {};".format(try_set), printer=printer)

        # but accept what we're given
        printer("Taking what we can get...")
        result = show_do_query(cursor, "show variables where Variable_name = 'group_concat_max_len';" , printer=printer)
        max_group_concat_bytes = int(result[0]['Value'])

        # and see how many rows that is
        printer("How many of these will fit?")
        result = show_do_query(cursor, "select length(concat(md5('foo'),',')) as md5_bytes;" , printer=printer);
        md5_bytes = int(result[0]['md5_bytes'])

    rows = floor(max_group_concat_bytes / md5_bytes)
    printer("{} rows".format(rows))
    return rows

def get_max_id(cursor, table_name, id_col='id', printer=Prindenter()):

    target = 'max({})'.format(id_col)
    get_max_id = 'select {} from {};'.format(target, table_name)

    result = show_do_query(cursor, get_max_id, printer=printer)
    return result[0][target]


# recursive descent would be more optimal
# TODO: measure and see if it's worth the extra complexity (beware of max_group_concat_length)

# for now just walk the ranges and transfer any with diffs
def find_diff_intervals(remote_cursor, local_cursor, table_name, intervals, id_col='id', printer=Prindenter()):

    has_diffs = []

    printer("Examining table formats on either side]")
    with Indent(printer):
        local_columns = examine_columns(local_cursor, table_name, printer=printer)
        remote_columns = examine_columns(remote_cursor, table_name, printer=printer)

    printer("[Scanning intervals for changes]")
    with Indent(printer):
        for interval in intervals:

            printer("[Scanning {}]".format(interval))
            with Indent(printer):
                local_fingerprint = md5_row_range(local_cursor, table_name, local_columns, interval,
                                                  id_col=id_col, printer=printer)

                remote_fingerprint = md5_row_range(remote_cursor, table_name, remote_columns, interval,
                                                   id_col=id_col, printer=printer)

            if local_fingerprint == remote_fingerprint:
                printer("{} NO SYNC NEEDED\n".format(interval))
            else:
                printer("{} NEEDS SYNC\n".format(interval))
                has_diffs.append(interval)

    return has_diffs

def is_equal(remote_cursor, local_cursor, table_name, printer=Prindenter()):
    printer("[Checking table equality for {}]".format(table_name))

    with Indent(printer):
        get_checksum = 'checksum table {};'.format(table_name)

        result = show_do_query(remote_cursor, get_checksum, printer=printer)
        remote_checksum = result[0]['Checksum']

        result = show_do_query(local_cursor, get_checksum, printer=printer)
        local_checksum = result[0]['Checksum']

        if remote_checksum != local_checksum:
            printer("WARNING: {} differs, even after sync!".format(table_name))
            return False
        else:
            return True
            printer("{} is identical on either side".format(table_name))

def warn_if_not_equal(remote_cursor, local_cursor, table_name, printer=Prindenter()):
    if is_equal(remote_cursor, local_cursor, table_name, printer):
        printer("{} is identical on either side".format(table_name))
    else:
        printer("WARNING: {} differs, even after sync!".format(table_name))

# this works for tables that only experience INSERTs, it just checks on max(id)
# and syncs the deficit
def pull_missing_ids(table_name, cli_args, local_args, remote_connection, printer=Prindenter()):

    target = 'max(id)';
    get_max_id = 'select {} from {};'.format(target, table_name)

    # main() doesn't pass-in a local connection because if the connection is held open
    # then mysqlload_local (below) can't make its own separate connection
    with LocalConnection(local_args) as local_connection:
        with local_connection.cursor() as cursor:
            printer("Finding max(id) for {}.{}".format(table_name, cursor.connection.db))
            with Indent(printer):
                result = show_do_query(cursor, get_max_id, printer=printer)
                begin = result[0][target] or 0

    with remote_connection.cursor() as cursor:
        printer("Finding max(id) for {}.{}".format(table_name, cursor.connection.db))
        with Indent(printer):
            result = show_do_query(cursor, get_max_id, printer=printer)
            end = result[0][target] or 0

    if end == begin:
        printer("Nothing to sync")

    # check for local changes beyond max_id for remote db and clobber them (this is a one-way sync)
    elif end < begin:
        with LocalConnection(local_args) as local_connection:
            with local_connection.cursor() as cursor:
                printer("Downstream db has more rows, deleting them.")
                delete = 'delete from {} where id > {};'.format(table_name, end)
                with Indent(printer):
                    result = show_do_query(cursor, delete, printer=printer)
    else:
        printer("Upstream db has more rows, pulling them.")

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

    with LocalConnection(local_args) as local_connection:
        with local_connection.cursor() as local_cursor:
            with remote_connection.cursor() as remote_cursor:
                warn_if_not_equal(remote_cursor, local_cursor, table_name, printer=printer)

    # return max id
    return end

# used for tables that don't have a more specific strategy
def general_sync(table_name, interval_size, cli_args, local_args, remote_connection, printer=Prindenter()):

    # Check to see if work needs to be done
    with LocalConnection(local_args) as local_connection:
        with local_connection.cursor() as local_cursor:
            with remote_connection.cursor() as remote_cursor:

                if is_equal(remote_cursor, local_cursor, table_name, printer):
                    printer("{} is identical on either side".format(table_name))
                    return

                printer("[Syncing new rows for table {}]".format(table_name))
                with Indent(printer):

                    # get any new rows
                    max_id = pull_missing_ids(table_name, cli_args, local_args, remote_connection, printer=printer)

                if is_equal(remote_cursor, local_cursor, table_name, printer):
                    printer("{} is identical on either side".format(table_name))
                    return

                printer("[Scanning for diffs in table {}]".format(table_name))
                with Indent(printer):

                    # find which id-ranges have changes that need syncing
                    remote_max = get_max_md5_rows(remote_cursor, printer=printer)
                    local_max = get_max_md5_rows(local_cursor, printer=printer)
                    interval_size = min(remote_max, local_max, interval_size)

                    intervals = make_intervals(0, max_id, interval_size)

                    diff_intervals = find_diff_intervals(remote_cursor, local_cursor, table_name, intervals,
                                                         printer=printer)

    printer("[Transferring for diffs in table {}]".format(table_name))
    with Indent(printer):

        for interval in diff_intervals:

            condition = 'id >= {} and id <= {}'.format(interval.start, interval.end)

            # dump remote data
            mysqldump_data_remote(cli_args, table_name, condition, printer=printer)

            # clear old rows from local
            delete = 'delete from {} where {};'.format(table_name, condition)
            with LocalConnection(local_args) as local_connection:
                with local_connection.cursor() as cursor:
                    show_do_query(cursor, delete, printer=printer)

            # load new rows into local
            mysqlload_local(cli_args, table_name, printer=printer)

    # warn if not equal
    with LocalConnection(local_args) as local_connection:
        with local_connection.cursor() as local_cursor:
            with remote_connection.cursor() as remote_cursor:
                warn_if_not_equal(remote_cursor, local_cursor, table_name, printer=printer)
