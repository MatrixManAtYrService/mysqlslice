#! /usr/bin/env bash
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

set -euo pipefail

# control_before, experimental_before, control_after, experimental_after, test_name
report() {

    echo "control_before $1"
    echo "experimental_before $2"
    echo "control_after $3"
    echo "experimental_after $4"

    if [[ "$1" == "$2" ]] ; then
        echo "Both before images match, was this supposed to be a null test?"
            exit 1
    fi

    if [[ "$1" != "$3" ]] ; then
        echo "'things_upstream' changed.  This is supposed to be a one-way sync where only 'things_downstream' changes."
            exit 2
    fi

    if [[ "$3" != "$4" ]] ; then
        echo "After the sync, images were not identical"
            exit 2
    fi

    echo "Test $5 passed"

}

echo "Initializing some tables in 'things_upstream', same data goes in 'things_downstream'"
echo "Making changes only in things_upstream"
mysql -uroot -ptest -e "source sql/init.sql;" | sed 's/^/    /g'

echo "Here they are before the sync"
mysql -uroot -ptest -t -e "source sql/show_everything.sql;" | sed 's/^/    /g'

control_before="$(mysql -uroot -ptest -e "use things_upstream; source sql/show_one_side.sql;" | md5sum)"
experimental_before="$(mysql -uroot -ptest -e "use things_downstream; source sql/show_one_side.sql;" | md5sum)"

echo "Now syncing from 'things_upstream' to 'things_downstream'"
pull_slice | sed 's/^/    /g'

control_after="$(mysql -uroot -ptest -e "use things_upstream; source sql/show_one_side.sql;" | md5sum)"
experimental_after="$(mysql -uroot -ptest -e "use things_downstream; source sql/show_one_side.sql;" | md5sum)"

echo "Here they are after the sync"
mysql -uroot -ptest -t -e "source sql/show_everything.sql;" | sed 's/^/    /g'

report "$control_before" "$experimental_before" "$control_after" "$experimental_after" "Sync-after-changes"

echo "Nuking downstream database"
mysql -uroot -ptest -t -e "drop database things_downstream; create database things_downstream;"

echo "Pulling schema from upstream"
pull_schema | sed 's/^/    /g'

control_before="$(mysql -uroot -ptest -e "use things_upstream; source sql/show_one_side.sql;" | md5sum)"
experimental_before="$(mysql -uroot -ptest -e "use things_downstream; source sql/show_one_side.sql;" | md5sum)"

echo "Syncing into empty schema"
pull_slice | sed 's/^/    /g'

control_after="$(mysql -uroot -ptest -e "use things_upstream; source sql/show_one_side.sql;" | md5sum)"
experimental_after="$(mysql -uroot -ptest -e "use things_downstream; source sql/show_one_side.sql;" | md5sum)"

report "$control_before" "$experimental_before" "$control_after" "$experimental_after" "Sync-after-nuke"
