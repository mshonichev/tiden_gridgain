#!/usr/bin/env python3
#
# Copyright 2017-2020 GridGain Systems.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
from unittest.mock import patch
from os import listdir
from os.path import join, exists, dirname
from difflib import unified_diff
from shutil import rmtree
import pytest

@pytest.mark.parametrize("test_file_name,class_count", [
    ("visor_parser_test_empty_model", 0),
    ("visor_parser_test_bank_model", 205),
])

def test_visor_parser(test_file_name, class_count):
    import visor_parser
    working_directory = join(dirname(__file__), 'res', 'visor_parser')
    print(join(working_directory, '%s_in.txt' % test_file_name))
    testargs = [
        "prog",
        "--config_path=%s" % join(working_directory, '%s_in.txt' % test_file_name),
        "--out_dir=%s" % working_directory,
        "--no-anonymous_caches_and_group",
    ]
    with patch.object(sys, 'argv', testargs):
        visor_parser.main()
    try:
        import xml.etree.ElementTree as ET
        doc = ET.parse(join(working_directory, 'model', 'caches.xml'))
    except ValueError:
        assert False, "XML format check return error"
    except Exception:
        print('Validate XML format step was skipped')
    # Assert to out config
    dump_diff = unified_diff(
        open(join(working_directory, '%s_out.txt' % test_file_name)).readlines(),
        open(join(working_directory, 'model', 'caches.xml')).readlines(),
        fromfile=join(working_directory, 'visor_parser_test_out_test1.txt'),
        tofile=join(working_directory, 'model', 'caches.xml')
    )
    assert ''.join(dump_diff) == '', \
        "files %s,%s not match " % (
            join(working_directory, 'visor_parser_test_out_test1.txt'),
            join(working_directory, 'model', 'caches.xml')
        )
    # Assert to count .class and .java
    assert class_count == len(listdir(join(working_directory, 'model', 'java'))) == len(listdir(
        join(working_directory, 'model', 'classes'))), "Count of java's and class's files is not equal"
    # Assert to exist jar and json file
    assert exists(join(working_directory, 'model', 'json_model.json'))
    assert exists(join(working_directory, 'model', 'model.jar'))

    rmtree(join(working_directory, 'model'))


def test_visor_parser_anonymous_caches():
    import visor_parser
    test_file_name = "visor_parser_test_empty_model"

    working_directory = join(dirname(__file__), 'res', 'visor_parser')
    print(join(working_directory, '%s_in.txt' % test_file_name))
    testargs = [
        "prog",
        "--config_path=%s" % join(working_directory, '%s_in.txt' % test_file_name),
        "--out_dir=%s" % working_directory,
        "--no-anonymous_caches_and_group",
    ]

    # test '--no-option' results in option equal to False
    with patch.object(sys, 'argv', testargs):
        visor_parser.parse_args()
        assert False == visor_parser.options.anonymous_caches_and_group

    # test '--option' results in option equal to True
    testargs[-1] = "--anonymous_caches_and_group",
    with patch.object(sys, 'argv', testargs):
        visor_parser.parse_args()
        assert True == visor_parser.options.anonymous_caches_and_group

    # and for that specific option, not mentioning it in the arguments list results in default value True
    del testargs[-1]
    with patch.object(sys, 'argv', testargs):
        visor_parser.parse_args()
        assert True == visor_parser.options.anonymous_caches_and_group

