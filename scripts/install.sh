#!/bin/bash

# Exit on any error
set -e

SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"

# shellcheck disable=SC1090
source "${SCRIPTPATH}/utils.sh"

# source python3
source /Users/jordanmance/venv/default-3.9/bin/activate

notify "Uninstalling previous figgy-lib"


pip uninstall figgy-lib --yes

notify "Building & Installing figgy-lib $(pwd)"


python "${SCRIPTPATH}"/../setup.py install

success "Install complete."