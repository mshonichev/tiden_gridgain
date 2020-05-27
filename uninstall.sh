#/bin/bash

sudo pip3.7 uninstall -y tiden_gridgain twine keyring keyrings.alt


# remove remnants of previous `setup.py develop`
for script_name in visor_parser; do
  for bin_path in /usr/local/bin /usr/bin ~/.local/bin; do
    if [ -f $bin_path/$script_name.py ]; then
      sudo rm -f $bin_path/$script_name.py
    fi
  done
done
