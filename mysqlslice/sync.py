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
def pull_foo(local_args, remote_connection, printer=Prindenter()):

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

def md5_row_range(cursor, table_name, interval, id_col='id', printer=Prindenter()):

    printer("[Fingerprinting " + table_name
             + ".{table_name} across rows where {id_col} in {interval}]".format(**vars()))

    with Indent(printer):
        result = show_do_query(cursor,
                               "SELECT COLUMN_NAME, IS_NULLABLE, COLUMN_TYPE, COLLATION_NAME "
                                + "FROM information_schema.columns "
                                + "WHERE table_schema='{}' ".format(cursor.connection.database)
                                + "AND table_name='{}';".format(table_name),
                               printer=printer)

        column_names = []
        column_conversions= []

        for column in result:

            # keep track of the raw names
            name = column['COLUMN_NAME']
            column_names.append(name)

            # make the column representation concatenate-friendly
            converted = column['COLUMN_NAME']

            if column['IS_NULLABLE'] == 'YES':
                converted = "IFNULL({}, 'NULL')".format(converted)

            if column['COLLATION_NAME'] not in ['NULL', 'utf8_general_ci']:
                converted = "BINARY {}".format(converted)

            # your data may deviate in new and exciting ways
            # handle them here ...

            column_conversions.append('{} as {}'.format(converted, name))

        # column names for the target table
        columns_str = ",".join(column_names)

        # concat-friendly conversions for the target table
        converted_columns_str = ",".join(column_conversions)

        # hash the row-range
        start = interval.start
        end = interval.end
        result = show_do_query(cursor,
                               "SELECT md5(group_concat(" + columns_str + ")) as fingerprint from"
                               + "  (select " + converted_columns_str + " from " + table_name
                               + "   where {id_col} >= {begin} and {id_col} < {end}".format(**vars())
                               + "   order by id) as " + table_name
                               + " GROUP BY 1 = 1;", # this is dumb, but I don't know how to dispense with it
                               printer=printer)

        return result[0]['fingerprint']

# row sync relies on group_concat, which silently truncates the output once it reaches
# group_concat_max_len bytes long.
# Interrogate the target server to see how many rows we can get away with.
def get_max_md5_rows(cursor, try_set=1000001 * 33, printer=Prindenter()):
    # 32 bytes for the md5 plus 1 for the comma times a million rows

    # limiting it here because I'd prefer too many small queries over a few monsters
    # that ties up the server with no gaps.  This may be unnecessarily cautious, go bigger at your own risk.

    # hell, this is all at your own risk

    printer("[How many rows is {} willing to hash at a time?]".format(cursor.connection.host))
    with Indent(printer):

        # try to ask for enough space for 1 million rows at a time
        printer("Asking for lots lof space...")
        result = show_do_query(cursor, "set session group_concat_max_len = {};".format(try_set)

        # but accept what we're given
        printer("Taking what we can get...")
        result = show_do_query(cursor, "show variables where Variable_name = 'group_concat_max_len';" , printer=printer)
        max_group_concat_bytes = result[0]['Value']

        # and see how many rows that is
        printer("How many of these will fit?")
        result = show_do_query(cursor, "select length(concat(md5('foo'),','));" , printer=printer)
        md5_bytes = result[0]['md5_bytes']# for the comma

    rows = floor(max_group_concat_bytes / md5_bytes)
    printer(rows + " rows")
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

    for interval in intervals:
        local_fingerprint = md5_row_range(local_cursor, table_name, interval, id_col=id_col, printer=printer)
        remote_fingerprint = md5_row_range(remote_cursor, table_name, interval, id_col=id_col, printer=printer)

        if local_fingerprint == remote_fingerprint:
            printer("{} needs NO sync".format(interval))
        else:
            printer("{} needs sync".format(interval))
            has_diffs.append(interval)

    return has_diffs

def warn_if_not_equal(remote_cursor, local_cursor, table_name, printer=Printdenter()):
    printer("[Checking table equality for {}]".format(table_name))

    with Indent(printer):
        get_checksum = 'checksum table {};'.format(table_name)

        result = show_do_query(remote_cursor, get_checksum, printer=printer)
        remote_checksum = result[0]['Checksum']

        result = show_do_query(local_cursor, get_checksum, printer=printer)
        local_checksum = result[0]['Checksum']

        if remote_checksum != local_checksum:
            printer("WARNING: {} differs, even after sync!".format(table_name))
        else:
            printer("{} is identical on either side".format(table_name))



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

    with LocalConnection(local_args) as local_connection:
        with local_connection.cursor() as local_cursor:
            with remote_connection.cursor() as remote_cursor:
                warn_if_not_equal(remote_cursor, local_cursor, table_name, printer=printer):

    # return max id
    return end

# used for tables that don't have a more specific strategy
def general_syc(table_name, interval_size, local_args, remote_connection, printer=Prindenter()):

    printer("[Syncing new rows for table {}]".format(table_name)
    with Indent(printer):

        # get any new rows
        max_id = pull_missing_ids(table_name, local_args, remote_connection, printer=printer)

    printer("[Scanning for diffs in table {}]".format(table_name)
    with Indent(printer):

        with LocalConnection(local_args) as local_connection:
            with local_connection.cursor() as local_cursor:
                with remote_connection.cursor() as remote_cursor:

                    # find which id-ranges have changes that need syncing
                    remote_max = get_max_md5_rows(remote_cursor, printer=printer)
                    local_max = get_max_md5_rows(local_cursor, printer=printer)
                    interval_size = min(remote_max, local_max, interval_size)

                    intervals = make_intervals(0, max_id, interval_size)

                    diff_intervals = find_diff_intervals(remote_cursor, local_cursor, table_name, intervals,
                                                         printer=printer)

    printer("[Transferring for diffs in table {}]".format(table_name)
    with Indent(printer):

        for interval in diff_intervals:

            condition = 'id =< {} and id >= {}'.format(interval.start, interval.end)

            # dump remote data
            mysqldump_data_remote(cli_args, table_name, condidtion, printer=printer)

            # clear old rows from local
            delete = 'delete from {} where {};'.format(table_name, condition)
            with LocalConnection(local_args) as local_connection:
                with local_connection.cursor() as cursor:
                    show_do_query(cursor, , printer=printer)

            # load new rows into local
            mysqlload_local(cli_args, table_name, printer=printer)

    # warn if not equal
    with LocalConnection(local_args) as local_connection:
        with local_connection.cursor() as local_cursor:
            with remote_connection.cursor() as remote_cursor:
                warn_if_not_equal(remote_cursor, local_cursor, table_name, printer=printer):
