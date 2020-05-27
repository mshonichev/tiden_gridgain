#!/usr/bin/env python3

import pytest
from datetime import datetime, timedelta

from tiden.zabbix_api import ZabbixApi, ZabbixApiException


def test_successful_connection():
    zapi = ZabbixApi('https://ggmon.gridgain.com/', 'ggqa', 'fysytb6Q')
    assert zapi.auth_key


def test_failed_connection():
    with pytest.raises(ZabbixApiException) as err_login:
        zapi = ZabbixApi('https://ggmon.gridgain.com/', 'ggqa', 'fysytb6Q1')
    assert err_login.value.args[0]['reason'] == 'Login name or password is incorrect.'


def test_metric_collection():
    zapi = ZabbixApi('https://ggmon.gridgain.com/', 'ggqa', 'fysytb6Q')
    metrics = zapi.collect_metrics_from_servers(['172.25.1.37'], ['Available memory'],
                                                datetime.now() - timedelta(minutes=60),
                                                datetime.now())
    assert metrics
