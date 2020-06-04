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

import argparse
import sys
from git import Repo
from pathlib import Path
import os
import subprocess
import datetime
import shutil
import glob
import ftplib


# Command line parser
def create_parser():
    p = argparse.ArgumentParser(add_help=False, usage=argparse.SUPPRESS)
    p.add_argument('--src', required=True, type=str, help="AI source directory")
    p.add_argument('--res', nargs='+', required=True, help="Pairs branch1:revision1 branch2:revision2")
    p.add_argument('--upload', action='store_const', const=True, help="Upload artifacts to ftp or not")
    p.add_argument('--ip_addr', help="FTP ip address")
    p.add_argument('--user', help="FTP username")
    p.add_argument('--password', help="FTP password")
    p.add_argument('--ftp_rd', help="FTP dir")
    return p


def convert_input_to_dict(pairs_list):
    pairs_dict = {}
    for pair in pairs_list:
        splitted_pair = pair.split(':')
        branch = splitted_pair[0]
        if len(splitted_pair) > 1:
            revision = splitted_pair[1]
        else:
            revision = None
        pairs_dict[branch] = revision
    return pairs_dict


def upload_to_ftp(art_dir='', ftp_ip='', ftp_user='', ftp_password='', remote_ftp_dir=''):
    ftp_list = [art_dir, ftp_ip, ftp_user, ftp_password, remote_ftp_dir]
    if not all(ftp_list):
        print('Please check input parameters!\n'
              'ftp ip address\n'
              'ftp username\n'
              'ftp password\n'
              'ftp remote dir')
        sys.exit(1)
    os.chdir(str(art_dir))
    ftp = ftplib.FTP(ftp_ip)
    ftp.login(ftp_user, ftp_password)
    print('==== Connect to {ftp_ip} ===='.format(ftp_ip=ftp_ip))
    try:
        ftp.cwd(str(remote_ftp_dir))
    except ftplib.error_perm:
        # If directory not exists
        ftp.mkd(str(remote_ftp_dir))
        ftp.cwd(str(remote_ftp_dir))
    for f in os.listdir(str(art_dir)):
        print('===== Uploading file {f} to {ftp_ip} ====='.format(f=f,
                                                                  ftp_ip=ftp_ip))
        ftp.storbinary("STOR " + f, open(f, 'rb'))
    ftp.quit()


def main():
    """
    Prepare Apache Ignite builds for benchmarking
    """
    # Create parser
    parser = create_parser()
    namespace = parser.parse_args(sys.argv[1:])
    source_directory = namespace.src
    wb_upload = namespace.upload
    ftp_addr = ''
    ftp_user = ''
    ftp_pwd = ''
    ftp_base_dir = ''
    # Will be uploaded or not?
    if wb_upload:
        ftp_addr = namespace.ip_addr
        ftp_user = namespace.user
        ftp_pwd = namespace.password
        ftp_base_dir = namespace.ftp_rd

    if not Path(str(source_directory)).exists():
        print(' ==== Error! No such directory {s_directory} ===='.format(s_directory=source_directory))
    res = namespace.res
    b_dct = convert_input_to_dict(res)
    print(' ==== Branches and revisions {b_dct} ===='.format(b_dct=b_dct))
    current_dir = Path.cwd()
    timestamp = datetime.datetime.now().strftime("%y%m%d-%H%m%S")
    res_dir = Path(current_dir).joinpath('tmp-{timestamp}'.format(timestamp=timestamp))
    Path.mkdir(res_dir)
    print('== Results will be saved {res_dir} =='.format(res_dir=res_dir))
    print('==== Current directory is {current_dir} ===='.format(current_dir=current_dir))
    os.chdir(source_directory)
    print('==== Go to {source_directory} ===='.format(source_directory=source_directory))
    r = Repo(source_directory)
    art_number = 0
    info_dict = {}
    # Branches loop
    for b in b_dct:
        repo_heads = r.heads
        repo_heads[b].checkout()
        if b_dct[b]:
            # Go to specific commit
            r.head.reset(b_dct[b], index=True, working_tree=True)
        print('Current commit is {commit_sha}'.format(commit_sha=r.head.commit.hexsha))
        print('Current branch is {act_branch}'.format(act_branch=r.active_branch))

        print('===== Build AI =====')
        # Build Apache ignite
        subprocess.call(['mvn', '-q', 'clean', 'install', '-Pall-java,all-scala,licenses', '-DskipTests'])
        subprocess.call(['mvn', '-q', 'initialize', '-Prelease'])
        # Result directory
        target_bin = Path(source_directory).joinpath('target', 'bin')

        os.chdir(str(target_bin))
        build = glob.glob('apache-ignite-fabric-*')[0]
        print('== Current build is {b} =='.format(b=build))
        if art_number == 0:
           order = 'base'
           art_number += 1
        else:
            order = 'test'
        art_name = 'apache-ignite-{order}-{act_br}-{commit_sha}.zip'.format(order=order,
                                                                            act_br=r.active_branch,
                                                                            commit_sha=str(r.head.commit.hexsha)[0:9])
        info_dict[str(r.head.commit.hexsha)[0:9]] = order
        print('== Current art. mane is {art_name} =='.format(art_name=art_name))
        shutil.move(build, art_name)
        shutil.move(art_name, str(res_dir))
        os.chdir(str(source_directory))

    if wb_upload:
        rem_line = ""
        for item in info_dict.keys():
            rem_line += '-{i}-{iinfo}'.format(i=str(item), iinfo=str(info_dict[item]))
        # Upload to ${ftp_base_dir}/bench-${rem_line}
        upload_to_ftp(art_dir=res_dir,
                      ftp_ip=ftp_addr,
                      ftp_user=ftp_user,
                      ftp_password=ftp_pwd,
                      remote_ftp_dir=Path(ftp_base_dir).joinpath('bench{rl}'.format(rl=rem_line)))


if __name__ == '__main__':
    main()
