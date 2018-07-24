import sys
import textwrap
import argparse
import subprocess
import os
import re
from pprint import pformat
from sh import bash, awk, netstat, mysql
from mysqlslice.mysql import LocalArgs, RemoteArgs

# Parsing Command Line Aguments
# =============================

def parse_pull_args(desc=None):

    parser = argparse.ArgumentParser(description=desc,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    # some defaults for the local database
    dl_user = 'root'
    dl_database = 'things_downstream'
    dl_password = 'test'

    # try to determine the mysql socket path
    dl_socket = ""
    if "linux" in sys.platform:
        dl_socket= str(awk(netstat('-ln'), '/mysql(.*)?\.sock/ { print $9 }')).strip()
    elif sys.platform == "darwin":
        dl_socket= str(awk(netstat('-an'), '/mysql(.*)?\.sock/ { print $5 }')).strip()

    # if we don't find a file, make it a required parameter
    if not os.path.exists(dl_socket):
        dl_socket=None

    # some defaults for the remote database
    # typically this would be a remote server--using localhost for testing
    dr_database = 'things_upstream'
    dr_user = 'root'
    dr_password = 'test'
    dr_host = '127.0.0.1'

    parser.add_argument('--local-user',            default=dl_user)
    parser.add_argument('--local-password',        default=dl_password)
    parser.add_argument('--local-database',        default=dl_database)
    parser.add_argument('--local-socket',          default=dl_socket)
    parser.add_argument('-u', '--remote-user',     default=dr_user)
    parser.add_argument('-p', '--remote-password', default=dr_password)
    parser.add_argument('-o', '--remote-host',     default=dr_host)
    parser.add_argument('-d', '--remote-database', default=dr_database)
    parser.add_argument('-c', '--cipher')

    return parser.parse_args()

# Printing Messages to the Caller
# ===============================

# print status to stderr so that only the requested value is written to stdout
# (the better for consumption by a caller in code)
# default to a four-space indent
class Prindenter:
    def __init__(self, indent=4, file=sys.stderr):
        self.indent = indent
        self.at_line_begin = True
        self.file = file

    def __call__(self, msg, end='\n'):

        if self.at_line_begin:
            this_indent = self.indent
        else:
            this_indent =  0

        if end is '':
            self.at_line_begin = False
        else:
            self.at_line_begin = True

        print(textwrap.indent(msg.__str__(), ' ' * this_indent), file=self.file, end=end)

# Increments the intent depth for a Prindenter
class Indent:
    def __init__(self, printer):
        self.printer = printer

    def __enter__(self):
        self.printer.indent += 4

    def __exit__(self, type, value, traceback):
        self.printer.indent -= 4


# Executing External Commands
# ===========================

# print a bash command and its result
def run_in_bash(command,
                run=lambda cmd : bash(['-c', cmd]),
                printer=Prindenter()):

    with Indent(printer):
        printer('[Command]')
        with Indent(printer):
            printer(command)
        printer('[Output]')
        with Indent(printer):
            # execute and print output
            result = run(command)
            printer(repr(result))
    return result

def mysqldump_data_remote(slice_args, table_name, condition, append=False, printer=Prindenter()):

    outfile = table_name + '.sql'
    printer('[Dumping {} from {}.{} where {} into {}/{}]'.format(table_name,
                                                                 slice_args.remote_host,
                                                                 slice_args.remote_database,
                                                                 condition,
                                                                 os.getcwd(),
                                                                 outfile))

    # build command string
    format_args = { 'table'     : table_name,
                    'condition' : condition,
                    'file'      : outfile }

    # if batch processing, append to file instead of making a new one
    if append:
        redirect = '>>'
    else:
        redirect = '>'

    format_args.update(slice_args.__dict__) # use key-names from argparse
    command = ' '.join(['mysqldump',
                        '--compress',
                        '-h{remote_host}',
                        '-u{remote_user}',
                        '-p{remote_password}',
                        '{remote_database}',
                        '{table}',
                        '--no-create-info',
                        '--lock-tables=false',
                        '--set-gtid-purged=OFF',
                        '--where=\'{condition}\'',
                        redirect, '{file}',
                       ]
                      ).format(**format_args)

    return run_in_bash(command, printer=printer)

# split a dump in pieces to avoid connection timeout issues
def mysqldump_data_remote_batches(slice_args, table_name, batch_size, max_id,
                                  min_id=0, condition=None, printer=Prindenter()):

    boundaries = list(range(min_id, max_id, batch_size))
    intervals = [ (x, x + batch_size - 1) for x in boundaries ]

    printer("[Dump proceeding across {} batches]".format(len(intervals)))
    with Indent(printer):

        first_batch = True
        for interval in intervals:

            # modify the condition for a smaller dump
            if condition:
                restricted_condition = condition + " and id >= {} and id <= {}".format(interval[0], interval[1])
            else:
                restricted_condition = "id >= {} and id <= {}".format(interval[0], interval[1])

            # dump in append mode for all but first batch
            if first_batch:
                first_batch = False
                mysqldump_data_remote(slice_args,
                                      table_name,
                                      restricted_condition,
                                      printer=printer)
            else:
                mysqldump_data_remote(slice_args,
                                      table_name,
                                      restricted_condition,
                                      append=True,
                                      printer=printer)


def mysqldump_schema_nofk_remote(slice_args, outfile, printer=Prindenter()):

    printer('[Dumping the schema without foreign keys '
            'from {}.{} into {}/{}]'.format(slice_args.remote_host,
                                            slice_args.remote_database,
                                            os.getcwd(),
                                            outfile))

    with Indent(printer):
        printer("You usually want those foreign keys.\n"
                "If you later use it for anything but slice testing,\n"
                "be sure to first rebuild this schema.")

    # build command string
    format_args = { 'file' : outfile }

    format_args.update(slice_args.__dict__) # use key-names from argparse
    command = ' '.join(['mysqldump',
                        '-h{remote_host}',
                        '-u{remote_user}',
                        '-p{remote_password}',
                        '{remote_database}',
                        '--lock-tables=false',
                        '--set-gtid-purged=OFF',
                        '--no-data',

                        # see https://stackoverflow.com/a/50010817/1054322 for more about this
                        '|', 'sed', ''' '$!N;s/^\(\s*[^C].*\),\\n\s*CONSTRAINT.*FOREIGN KEY.*$/\\1/;P;D' '''
                        '|', 'grep', '-v', '\'FOREIGN KEY\'',

                        '>', '{file}',
                       ]
                      ).format(**format_args)

    return run_in_bash(command, printer=printer)


def mysqlload_local(slice_args, table_or_file_name, printer=Prindenter()):

    # derive file name if table name was provided
    if re.match(r'.*\.sql$', table_or_file_name):
        infile = table_or_file_name
    else:
        infile = table_or_file_name + '.sql'

    printer('[Loading {} from {} into {}]'.format(infile,
                                                 '{}/{}'.format(os.getcwd(), infile),
                                                 slice_args.local_database))
    # build command string
    format_args =  { 'file' : infile }
    format_args.update(slice_args.__dict__) # use key-names from argparse
    command = ' '.join(['mysql',
                        '-u{local_user}',
                        '-p{local_password}',
                        '-D{local_database}',
                        '-e\'source {file};\'',
                       ]
                      ).format(**format_args)

    return run_in_bash(command,
                       printer=printer)

# pretty printer for query execution
def show_do_query(cursor,
        query,
        do=lambda cursor, query: cursor.execute(query),
        get=lambda cursor: cursor.fetchall(),
        printer=Prindenter()):

    printer('[MySQL @ {}, database: {}]'.format(cursor.connection.host, cursor.connection.db))
    with Indent(printer):
        printer('[Query]')
        with Indent(printer):
            printer(textwrap.dedent(query))
            do(cursor, query)
        printer('[Result]')
        with Indent(printer):
            result = get(cursor)
            printer(pformat(result))
    return result
