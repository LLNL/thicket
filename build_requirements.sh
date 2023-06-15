#!/bin/bash

set -e

CURR_PY_INTERPRETER="python3"

if [[ ! -z "${PYTHON_INTERPRETER}" ]]; then
    CURR_PY_INTERPRETER=${PYTHON_INTERPRETER}
fi

${CURR_PY_INTERPRETER} setup.py egg_info > /dev/null
mv ./llnl_thicket.egg-info/requires.txt .
mv requires.txt requirements.txt
rm -rf ./llnl_thicket.egg-info/

echo "New requirements.txt file generated!"
echo "Before trying to use it, remove any 'extras' headers"
echo "that might have been added."
