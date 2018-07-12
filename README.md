

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
    


