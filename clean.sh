#!/bin/bash

SCRIPT_PATH="$(cd "$(dirname $0)"; pwd)" # "

pushd $SCRIPT_PATH >/dev/null

find . -name __pycache__ -type d | xargs -I {} rm -rf {}
find . -name .pytest_cache -type d | xargs -I {} rm -rf {}
find . -name "*.egg-info" -type d | xargs -I {} rm -rf {}

rm -rf .eggs
rm -rf .tox
rm -rf build
rm -rf dist

# remove remnants of previous `setup.py develop`
for script_name in `ls -1 $SCRIPT_PATH/src/console/entry_points/*.py`; do
  if [ "$script_name" = "__init__.py" ]; then continue; fi
  for bin_path in /usr/local/bin /usr/bin ~/.local/bin; do
    if [ -f $bin_path/$script_name ]; then
      sudo rm -f $bin_path/$script_name
    fi
  done
done

if python3.7 -m site | grep tiden_gridgain 2>/dev/null 1>&2; then
  echo "Please, clean following from sys.path manually"
  python3.7 -m site | grep tiden_gridgain 2>/dev/null
fi

popd >/dev/null
