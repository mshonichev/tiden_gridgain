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

# VisorExport parser help
# Run: visor_parser.py --config_path=path_1 --out_dir=path_2
#       where: path_1 - path to VisorDump.cfg
#              path_2 - path to generate artifact
#       by default (can change by console key)
#           --gen_config=True
#           --gen_json=True
#           --gen_class=True
#           --gen_jar=True
#           --max_affinity_partition=1024
#           --zip_file_name=model.zip
#           --model_jar_path=None
#           --model_jar_class=parent_class

from zipfile import ZipFile
from os.path import basename
import json
from re import fullmatch, search, match, compile
import subprocess
from optparse import OptionParser
from os import path, makedirs, getenv
from shutil import rmtree
from random import randint

options = {}
config_lines = {}
json_data = {}
object_count = 0
stack_class_name = list()
stack_cache_name = list()


class OptionParserWithBooleanOptions(OptionParser):
    def add_boolean(self, dest, **kwargs):
        self.add_option('--%s' % dest, dest=dest, action='store_true', **kwargs)
        self.add_option('--no-%s' % dest, dest=dest, action='store_false', **kwargs)


def parse_args():
    global options
    parser = OptionParserWithBooleanOptions(
        usage="visor_parser.py --config_path=path_1 --out_dir=path_2\n"
              "   where: path_1 - path to VisorDump.cfg\n"
              "          path_2 - path to generate artifact\n"
              "   by default (can change by console key)\n"
              "          --gen-config=True\n"
              "          --gen-json=True\n"
              "          --gen-class=True\n"
              "          --gen-jar=True\n"
              "          --anonymous-caches-and-group=True\n"
              "          --max_affinity_partition=1024\n"
              "          --zip_file_name=model.zip\n"
              "          --model_jar_path=None\n"
              "          --model_jar_class=parent_class\n"

    )
    parser.add_option("--config_path", action='store', default=None)
    parser.add_option("--out_dir", action='store', default=None)
    parser.add_boolean("gen_config", default=True)
    parser.add_boolean("gen_json", default=True)
    parser.add_boolean("gen_class", default=True)
    parser.add_boolean("gen_jar", default=True)
    parser.add_boolean("anonymous_caches_and_group", default=True)
    parser.add_option("--zip_file_name", action='store', default="model.zip")
    parser.add_option("--skip_caches", action='store', default='ignite-sys-atomic-cache@default-ds-group')
    parser.add_option("--max_affinity_partition", action='store', default=1024)
    parser.add_option("--model_jar_path", action='store', default=None)
    parser.add_option("--model_jar_class", action='store', default='parent_class')
    parser.add_option("--another_region_cache_regexp", action='store', default=None)
    parser.add_option("--another_region_name", action='store', default='non-persistance')
    options, args = parser.parse_args()
    if options.config_path is None or options.out_dir is None:
        print("ERROR - Need to set path_1,path_2")
        print(parser.usage)
        exit(-1)

    print("Parser Options: " + str(options))


def prepare_work():
    global config_lines
    model_path = path.join('%s' % options.out_dir, 'model')
    path_java = path.join('%s' % options.out_dir, 'model', 'java')
    path_class = path.join('%s' % options.out_dir, 'model', 'classes')
    if path.exists(model_path):
        if path.exists(path_java):
            rmtree(path_java)
        if path.exists(path_class):
            rmtree(path_class)
        rmtree(model_path)
    makedirs(model_path)
    makedirs(path_java)
    makedirs(path_class)
    if path.exists(options.config_path):
        config_lines = [line.rstrip('\n') for line in open(options.config_path)]
    else:
        print("ERROR - Config file not found in %s", options.config_path)
        exit(-1)


def generate_config():
    global options, config_lines, stack_class_name, stack_cache_name
    cache_groups = {}
    if options.gen_config:
        with (open(path.join('%s' % options.out_dir, 'model', 'caches.xml'), 'w')) as configout:
            configout.write(
                '<?xml version="1.0" encoding="UTF-8"?>\n'
                '<beans xmlns="http://www.springframework.org/schema/beans"\n'
                '       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n'
                '       xmlns:util="http://www.springframework.org/schema/util"\n'
                '       xsi:schemaLocation="http://www.springframework.org/schema/beans\n'
                '           http://www.springframework.org/schema/beans/spring-beans-2.5.xsd\n'
                '           http://www.springframework.org/schema/util\n'
                '           http://www.springframework.org/schema/util/spring-util-2.0.xsd">\n'
                '   <util:list id="caches">\n'
            )
            cache_open = False
            query_open = False
            fields_open = False
            index_open = False
            re_config = compile('.*Configuration for cache: (.*)')
            re_atomicity = compile('^\s*Atomicity Mode: (.*)')
            re_affinity = compile('^\s*Affinity Backups: (.*)')
            re_affinity_partition = compile('^\s*Affinity Partitions: (.*)')
            re_mode = compile('^\s*Mode: (.*)')
            re_group = compile('^\s*Group: (.*)')
            re_sync_mode = compile('^\s*Write Synchronization Mode: (.*)')
            re_query = compile('^\s*Query Entities: .*')
            re_key = compile('^\s*Key Type: .*')
            re_table = compile('^\s*Table Name: (.*)')
            re_query_field = compile('^\s*Query Fields:.*')
            re_var = compile('\s*(.*) -> (.*)\s*')
            re_field = compile('^\s*Field Aliases:.*')
            re_index = compile('^\s*Index: .*')
            re_query_escape = compile('^\s*Query Escaped Names: .*')
            for i, line in enumerate(config_lines):
                if re_config.search(line):
                    cache_name = re_config.match(line).group(1)
                    if cache_open:
                        configout.write('       </bean>\n')
                    if fullmatch(cache_name, options.skip_caches):
                        cache_open = False
                    else:
                        cache_open = True
                        configout.write('       <bean class="org.apache.ignite.configuration.CacheConfiguration">\n')
                        if options.anonymous_caches_and_group:
                            cache_name = 'cache_%s' % i
                            stack_cache_name.append(i)
                        configout.write('           <property name="name" value="%s"/>\n' % cache_name)
                        if options.another_region_cache_regexp is not None:
                            if fullmatch(cache_name, options.another_region_cache_regexp):
                                configout.write(
                                    '           <property name="dataRegionName" value="%s"/>\n' %
                                    options.another_region_name)

                if cache_open:
                    if re_atomicity.search(line):
                        configout.write(
                            '           <property name="atomicityMode" value="%s"/>\n' % re_atomicity.match(line).group(
                                1))
                    if re_affinity.search(line):
                        configout.write(
                            '           <property name="backups" value="%s"/>\n' % re_affinity.match(line).group(1))
                    if re_affinity_partition.search(line):
                        configout.write(
                            '           <property name="affinity">\n')
                        configout.write(
                            '               <bean class="org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction">\n')
                        configout.write(
                            '                   <constructor-arg value="false"/>\n')
                        affinity_count = re_affinity_partition.match(line).group(1)
                        if int(affinity_count) > int(options.max_affinity_partition):
                            affinity_count = options.max_affinity_partition
                        configout.write(
                            '                   <constructor-arg value="%s"/>\n' % affinity_count)
                        configout.write(
                            '                </bean>\n')
                        configout.write(
                            '           </property>\n')
                    if re_mode.search(line):
                        configout.write(
                            '           <property name="cacheMode" value="%s"/>\n' % re_mode.match(line).group(1))
                    if re_group.search(line):
                        group_name = re_group.search(line).group(1)
                        if options.anonymous_caches_and_group:
                            if group_name in cache_groups:
                                group_name = cache_groups[group_name]
                            else:
                                cache_groups[group_name] = 'cache_group_%s' % i
                                group_name = 'cache_group_%s' % i
                        configout.write(
                            '           <property name="groupName" value="%s"/>\n' % group_name)
                    if re_sync_mode.search(line):
                        configout.write(
                            '           <property name="writeSynchronizationMode" value="%s"/>\n' % re_sync_mode.match(
                                line).group(1))
                    if re_query.search(line):
                        query_open = False
                    if re_key.search(line):
                        if index_open:
                            configout.write('                                       </list>\n'
                                            '                                   </constructor-arg>\n'
                                            '                                   <constructor-arg value="SORTED"/>\n'
                                            '                              </bean>\n')
                            configout.write('                           </list>\n'
                                            '                       </property>\n')
                            index_open = False
                        if not query_open:
                            configout.write('           <property name="queryEntities">\n'
                                            '                <list>\n'
                                            '                    <bean class="org.apache.ignite.cache.QueryEntity">\n')
                            configout.write(
                                '                       <property name="keyType" value="java.lang.Long"/>\n')
                            query_open = True
                        else:
                            configout.write('                    </bean>\n'
                                            '                    <bean class="org.apache.ignite.cache.QueryEntity">\n'
                                            '                       <property name="keyType" value="java.lang.Long"/>\n')
                    if re_table.search(line) and query_open:
                        value_name = '%s_%s' % (re_table.match(line).group(1), i)
                        if options.anonymous_caches_and_group:
                            value_name = 'cache_type_%s' % i
                        configout.write(
                            '                       <property name="valueType" value="%s"/>\n' % value_name
                        )
                        stack_class_name.append(i)
                    if re_query_field.search(line) and query_open:
                        fields_open = True
                        configout.write('                       <property name="fields">\n'
                                        '                           <map>\n')
                    if re_var.search(line) and fields_open:
                        configout.write('                               <entry key="%s" value="%s"/>\n' % (
                            re_var.match(line).group(1), re_var.match(line).group(2)))
                    if re_field.search(line) and fields_open:
                        fields_open = False
                        configout.write('                           </map>\n'
                                        '                       </property>\n')
                    if re_index.search(line) and query_open:
                        if not index_open:
                            configout.write('                       <property name="indexes">\n'
                                            '                           <list>\n'
                                            '                              <bean class="org.apache.ignite.cache.QueryIndex">\n'
                                            '                                   <constructor-arg>\n'
                                            '                                       <list>\n')
                            index_open = True
                        else:
                            configout.write('                                       </list>\n'
                                            '                                   </constructor-arg>\n'
                                            '                                   <constructor-arg value="SORTED"/>\n'
                                            '                              </bean>\n'
                                            '                              <bean class="org.apache.ignite.cache.QueryIndex">\n'
                                            '                                   <constructor-arg>\n'
                                            '                                       <list>\n')
                    if re_var.search(line) and index_open:
                        configout.write('                                           <value>%s</value>\n' % (
                            re_var.match(line).group(1)))
                    if re_query_escape.search(line) and query_open:
                        if index_open:
                            configout.write('                                       </list>\n'
                                            '                                   </constructor-arg>\n'
                                            '                                   <constructor-arg value="SORTED"/>\n'
                                            '                              </bean>\n')
                            configout.write('                           </list>\n'
                                            '                       </property>\n')
                            index_open = False
                        configout.write('                    </bean>\n'
                                        '                </list>\n'
                                        '           </property>\n')
                        query_open=False
                    # print(i,line)
            configout.write(
                '       </bean>\n'
                '   </util:list>\n'
                '</beans>\n'
            )


def type_constructor(class_name):
    dict_types = {
        'java.lang.Long': 'id_key',
        'java.lang.Integer': 'id_key.intValue()',
        'java.lang.Double': 'id_key.doubleValue()',
        'java.lang.Short': 'id_key.shortValue()',
        'java.lang.Byte': 'id_key.byteValue()',
        'java.lang.Float': 'id_key.floatValue()',
        'java.lang.String': '"%s"+id_key' % class_name,
        'java.lang.Object': 'id_key',
        'java.util.UUID': 'java.util.UUID.randomUUID()',
        'java.lang.Boolean': 'true' if randint(0, 1) == 0 else 'false',
        'java.util.Date': 'new java.util.Date()',
        'java.util.Calendar': 'java.util.Calendar.getInstance()',
        'java.math.BigDecimal': 'new java.math.BigDecimal(id_key)',
        'java.util.Set': 'new java.util.HashSet()'
    }
    return dict_types


def generate_class_plus_json():
    global config_lines, json_data, object_count, stack_class_name
    if path.exists(options.config_path) and options.gen_class:
        # Generate others
        map_values = []
        source_data_json = {}
        fields_open = False
        cache_count = -1;

        re_config = compile('.*Configuration for cache: (.*)')
        re_table = compile('^\s*Table Name: (.*)')
        re_query_field = compile('^\s*Query Fields:.*')
        re_var = compile('\s*(.*) -> (.*)\s*')
        re_field = compile('^\s*Field Aliases:.*')

        for line in config_lines:
            if re_config.search(line):
                cache_name = re_config.match(line).group(1)
                if not fullmatch(cache_name, options.skip_caches):
                    if options.anonymous_caches_and_group:
                        cache_name = 'cache_%s' % stack_cache_name.pop(0)
                cache_count += 1;
            if re_table.search(line):
                # print('%s' % match('.*Table Name: (.*)', line).group(1))
                table_name = re_table.match(line).group(1)
            if re_query_field.search(line):
                fields_open = True
                map_values = []
            if re_var.search(line) and fields_open:
                map_values.append((re_var.match(line).group(1), re_var.match(line).group(2)))
            if re_field.search(line) and fields_open:
                i = stack_class_name.pop(0)
                if options.anonymous_caches_and_group:
                    table_name = 'cache_type'
                fields_open = False
                source_data_json['%s_%s' % (table_name, i)] = cache_name
                object_count += 1
                # print(map_values)
                with (open(path.join('%s' % (options.out_dir), 'model', 'java', '%s_%s.java' % (table_name, i)),
                           'w')) as javaout:
                    if options.model_jar_path is not None:
                        javaout.write('import %s;\n' % options.model_jar_class)
                        javaout.write(
                            'public class %s_%s extends %s {\n' % (
                                table_name, i, options.model_jar_class.split(".")[-1]))
                    else:
                        javaout.write(
                            'public class %s_%s {\n' % (table_name, i))
                    for var in map_values:
                        javaout.write('    private %s %s;\n' % (var[1], var[0]))
                    javaout.write('\n    public %s_%s(java.lang.Long id_key) {\n' % (table_name, i))
                    # print constructor
                    dict_types = type_constructor(var[0])
                    for var in map_values:
                        try:
                            value = dict_types[var[1]]
                            javaout.write('         this.%s = %s;\n' % (var[0], value))
                        except KeyError:
                            print('KeyError for type in %s.%s , key - %s' % (cache_name, table_name, var[0]))
                    javaout.write('    }\n')
                    for var in map_values:
                        if var[0] == 'notify':
                            javaout.write('    public %s notify_a(){\n' % var[1])
                        elif var[0] == 'hashCode':
                            javaout.write('    public %s hashCode_a(){\n' % var[1])
                        else:
                            javaout.write('    public %s %s(){\n' % (var[1], var[0]))
                        javaout.write('         return %s;\n' % var[0])
                        javaout.write('    }\n')
                    javaout.write('     public String toString() {\n')
                    javaout.write('     return')
                    for var in map_values:
                        javaout.write(' %s.toString() +' % var[0])
                    javaout.write('"SberModel";\n')
                    javaout.write('    }\n')
                    javaout.write('     @Override\n')
                    javaout.write('     public int hashCode() {\n')

                    javaout.write('         return this.toString().hashCode();\n')
                    javaout.write('     }\n')
                    javaout.write('}')
                javaout.close()
        # generate json object
        json_data = json.dumps(source_data_json)


def pack():
    global json_data
    with open(path.join('%s' % options.out_dir, 'model', 'json_model.json'), 'w') as outfile:
        json.dump(json_data, outfile)
    java_home = getenv("JAVA_HOME")
    javac_home_os = path.join('%s' % java_home, 'bin', 'javac')
    jar_home_os = path.join('%s' % java_home, 'bin', 'jar')
    class_out_home = path.join('%s' % options.out_dir, 'model', 'classes', '')
    class_in_home = path.join('%s' % options.out_dir, 'model', 'java', '*.java')
    model_jar = path.join('%s' % options.out_dir, 'model', 'model.jar')
    model_directory_path = path.join('%s' % options.out_dir, 'model')
    # compile classes
    if object_count > 0:
        if options.model_jar_path is None:
            cp = ""
        else:
            cp = "-cp %s " % options.model_jar_path
        cmd = '"%s" %s -d %s %s' % (javac_home_os, cp, class_out_home, class_in_home)
        print("Running Javac: %s" % cmd)
        javac = subprocess.Popen(cmd, shell=True)
        javac.wait()
        if javac.returncode != 0:
            print("ERROR compiling model:")
            print(javac.stderr)
            exit(-2)
    # compile jar
    cmd = '"%s" cvf %s -C %s .' % (jar_home_os, model_jar, class_out_home)
    print("Running Jar: %s" % cmd)
    jar = subprocess.Popen(cmd, shell=True)
    jar.wait()
    with ZipFile('%s' % path.join(model_directory_path, options.zip_file_name), 'w') as myzip:
        myzip.write('%s' % path.join(model_directory_path, 'caches.xml'), 'caches.xml')
        myzip.write('%s' % path.join(model_directory_path, 'json_model.json'), 'json_model.json')
        myzip.write('%s' % model_jar, basename(model_jar))


def main():
    parse_args()
    prepare_work()
    generate_config()
    generate_class_plus_json()
    pack()

# Main
if __name__ == '__main__':
    main()
