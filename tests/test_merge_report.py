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
import pytest
from os.path import join, dirname
from difflib import unified_diff
from tiden.util import load_yaml, save_yaml

@pytest.mark.parametrize("test_file_name_index", [
    (1),
])

def test_merge_reports_simple_merge(test_file_name_index, tmpdir):
    from tiden_gridgain.console.entry_points import merge_yaml_reports as merge_reports

    working_directory = join(dirname(__file__), 'res', 'merge_reports')
    output_directory = str(tmpdir)
    previous_report = join(working_directory, 'previous.{}.yaml'.format(test_file_name_index))
    current_report = join(working_directory, 'current.{}.yaml'.format(test_file_name_index))
    actual_result_report = join(output_directory, 'result.{}.yaml'.format(test_file_name_index))
    expected_result_report = join(working_directory, 'expected.{}.yaml'.format(test_file_name_index))
    exp = load_yaml(expected_result_report)
    expected_result_report = join(output_directory, 'expected.{}.yaml'.format(test_file_name_index))
    save_yaml(expected_result_report, exp)
    testargs = [
        "prog",
        "--output",
        actual_result_report,
        previous_report,
        current_report,
    ]
    with patch.object(sys, 'argv', testargs):
        merge_reports.main()
        diff = unified_diff(
            open(actual_result_report).readlines(),
            open(expected_result_report).readlines(),
            fromfile=actual_result_report,
            tofile=expected_result_report
        )
        assert ''.join(diff) == '', \
            "files %s,%s not match " % (
                actual_result_report,
                expected_result_report
            )

