This is a proof of concept / project template.  The idea is that since general-purpose database sync tools don't know your data, they can't make sync optimizations that you can.  This project syncs two databases

To run the sample:

    # you might need this for pymysql
    apt install libffi-dev libssl-dev

    # make a virtual env for this project
    python3 -m venv .venv
    source .venv/bin/activate
    pip install --upgrade pip setuptools

    # initialize the test databases
    mysql -uroot -ptest -e"source init.sql"

    # prepare the sync entrypoints
    python setup.py develop



pull_slice -u root -p test --remote-host 127.0.0.1
