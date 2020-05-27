#!/usr/bin/env python3

from tiden import hookimpl

@hookimpl
def tiden_get_applications_path():
    return ["tiden_gridgain.apps."]


@hookimpl
def tiden_get_plugins_path():
    from os.path import dirname, abspath, join
    from os import getcwd

    return [join(dirname(abspath(__file__)), "plugins")]

