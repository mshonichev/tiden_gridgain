#!/usr/bin/env python3

from os.path import join
from re import match
from uuid import uuid4

from tiden.assertions import *
from tiden.dockermanager import DockerManager
from tiden.sshpool import SshPool
from tiden.util import *


class IgniteTF(DockerManager):
    test_scripts = {
        "infinity": {
            "name": "infinity.py",
            "result_rows": 48
        },
        "ended": {
            "name": "ended.py",
            "result_rows": 48
        },
    }

    def __init__(self, config, ssh, cluster_configs):
        """
        :param cluster_configs:     cluster setup configurations
        """
        super().__init__(config, ssh)
        self.cluster_configs = cluster_configs

    def remove_all_images_and_container(self):
        """
        Remove all ignite TF images and running containers
        """
        self.remove_all_containers()
        self.remove_images(name_pattern="tf-executor:.*")
        self.remove_images(name_pattern="ignite-tf:.*")

    def remove_all_containers(self):
        """
        Remove all ignite TF containers
        """
        bases = ["tf-command", "tf-ps", "tf-attach", "tf-stop", "ignite-tf", "tf-executor"]
        pattern = "({})".format('|'.join(['({}.*)'.format(i) for i in bases]))
        self.remove_containers(name_pattern=pattern)

    def run_ignite(self, nodes_count, config_setup="default", hosts=None):
        """
        Run ignite-tf nodes containers
        """

        def run(host):
            image_id, log_file, name = self.run(
                self.image("ignite"), host,
                network="host",
                volume={self.cluster_configs[config_setup]["server_config_path"]: "/ignite-server.xml"}
            )

            host_images = self.running_containers.get(host, {})
            host_images[name] = {
                "id": image_id,
                "type": "ignite",
                "config": config_setup,
                "log": log_file,
                "name": name,
                "host": host
            }
            self.running_containers[host] = host_images

            # count ignite nodes by configurations
            host_ignite_containers = []
            for images in self.running_containers.values():
                for container in images.values():
                    if container["type"] == 'ignite' and container["config"] == config_setup:
                        host_ignite_containers.append(container)

            self.wait_for_text(host,
                               log_file,
                               "Topology snapshot",
                               lambda text: 'servers={}'.format(len(host_ignite_containers)) in text)

        if hosts is None:
            hosts = self.ssh.hosts
        if isinstance(nodes_count, int):
            base_hosts = hosts[:]
            while len(hosts) < nodes_count:
                hosts = hosts + base_hosts
            for i in range(nodes_count):
                run(hosts[i])

    def init_caches(self, config_setup="default", host=None):
        """
        Prepare caches from TF test script
        """

        if host is None:
            host = self.ssh.hosts[0]
        log_print("Init caches")
        logs_dir = self.config["rt"]["remote"]["test_dir"]
        log_file = "{}/{}".format(logs_dir, "init_caches_{}.log".format(str(uuid4())[:6]))

        if type(self.ssh) == SshPool:
            # remote
            java_path = "$JDK_ORA_18/bin/java"
        else:
            # local
            java_path = "java"

        jar_path = self.config["artifacts"]["cache-init"]["remote_path"]
        cmd = "export IGNITE_HOME={cache_path} && " \
              "cd {logs_dir}; " \
              " nohup " \
              " {java_path} " \
              "     -jar {jar_path} " \
              "     -c {config_path} " \
              "> {log_path} 2>&1 &".format(logs_dir=logs_dir,
                                           java_path=java_path,
                                           jar_path=jar_path,
                                           config_path=self.cluster_configs[config_setup]["client_config_path"],
                                           log_path=log_file,
                                           cache_path=logs_dir)
        self.ssh.exec_on_host(host, [cmd])[host]
        self.wait_for_text(host,
                           log_file,
                           'MNIST_CACHE created',
                           lambda text: 'MNIST_CACHE created' in text,
                           timeout=60 * 3)

    def start_executor(self, script_name="infinity", config_setup="default", commands=None, host=None,
                       name=None):
        """
        Start container with ignite-tf

        :param script_name:     script which be added in container
        :param config_setup:    node configuration
        :param commands:        start commands from container
        :param host:            host to run
        :param name:            container name
        :return:                running container obj
        """
        if host is None:
            host = self.ssh.hosts[0]

        scripts_home = self.config["rt"]["remote"]["test_module_dir"]
        script_path = join(scripts_home, self.test_scripts[script_name]["name"])

        log_print("Start executor")
        image_id, log_file, con_name = self.run(
            image_name=self.image("tf-executor"),
            host=host,
            network="host",
            volume={
                self.cluster_configs[config_setup]["client_config_path"]: "/tf/config/ignite-client.xml",
                script_path: "/tf/test_script/test.py"
            },
            commands=commands,
            kw_params={"name": name}
        )

        if not self.running_containers.keys():
            self.running_containers[host] = {}
        self.running_containers[host][con_name] = {
            "id": image_id,
            "type": "executor",
            "log": log_file,
            "name": con_name,
            "host": host
        }
        return self.running_containers[host][con_name]

    def start_script(self, script_name="infinity", config_setup="default", host=None):
        """
        Start script execution

        :param script_name:     script name from resources
        :param config_setup:    configuration
        :param host:            host to run
        :return:                started container obj
        """
        command = ["/tf/bin/ignite-tf.sh", "start",
                   "-c=/tf/config/ignite-client.xml", "MNIST_CACHE",
                   "/tf/test_script",
                   "python3", "test.py"]
        return self.start_executor(script_name=script_name,
                                   commands=command,
                                   host=host,
                                   config_setup=config_setup)

    def start_command(self, cmd):
        """
        Start executor container with command
        """
        log_print("Execute commands: '{}'".format(" ".join(cmd)))
        commands = ["/tf/bin/ignite-tf.sh"] + cmd
        return self.start_executor(commands=commands, name="tf-command")

    def get_command_info(self, cmd):
        """
        Start executor container with custom command
        """
        container = self.start_command(cmd)
        self.wait_for_text(container["host"], container["log"], "", lambda text: text, 10, interval=3)
        return self.get_logs(container)[0]

    def start_ps(self, config_setup="default", host=None):
        cmd = ["/tf/bin/ignite-tf.sh", "ps", "-c=/tf/config/ignite-client.xml"]
        return self.start_executor(commands=cmd, name="tf-ps", config_setup=config_setup)

    def start_attach(self, id, config_setup="default", host=None):
        """
        Start container with attach command
        :param id:              tf cluster ID
        :param config_setup:    configuration
        :param host:            host to run
        """
        cmd = ["/tf/bin/ignite-tf.sh", "attach", "-c=/tf/config/ignite-client.xml", id]
        return self.start_executor(commands=cmd, name="tf-attach", config_setup=config_setup)

    def start_stop(self, id, config_setup="default", host=None):
        """
        Start container with attach command
        :param id:              tf cluster ID
        :param config_setup:    configuration
        :param host:            host to run
        """
        cmd = ["/tf/bin/ignite-tf.sh", "stop", "-c=/tf/config/ignite-client.xml", id]
        return self.start_executor(commands=cmd, name="tf-stop", config_setup=config_setup)

    def stop_cluster(self, id, config_setup="default", host=None):
        """
        Run container with 'ignite-tf stop' command
        :param id:              cluster id
        :param config_setup:    configuration for connection
        :param host:            host to run
        """
        container = self.start_stop(id, config_setup="default", host=None)
        tiden_assert(self.wait_for_text(container["host"], container["log"], "", lambda text: text == ""),
                     "clusters list")
        del self.running_containers[container["host"]][container["name"]]

    def get_ps(self, config_setup="default"):
        """
        Run container with 'ignite-tf ps', and return command result
        :param config_setup:    configuration for connection
        :return:                command output
        """
        ps_container = self.start_ps(config_setup=config_setup)
        tiden_assert(self.wait_for_text(ps_container["host"], ps_container["log"], "", lambda text: text),
                     "clusters list")
        ps_list = self.get_logs(ps_container)

        del self.running_containers[ps_container["host"]][ps_container["name"]]
        ps_list = ps_list[0].strip("\n").split("\n")
        return [line for line in ps_list if line]

    def get_processes(self, hosts: list = None):
        """
        Find all python processes inside containers
        :param hosts:   hosts to search
        :return:        processes map:
                                {
                                    [host] : {
                                        [container name]: {
                                            [process1, process2]
                                        }
                                    }
                                }
        """
        if hosts is None:
            hosts = self.ssh.hosts
        cmd = "ps aux | grep python3 | grep -v grep"
        job_name_pattern = 'job:'
        task_name_pattern = "task:"
        output = {}
        for host in hosts:
            host_images = {}
            for name, image in self.running_containers.get(host, {}).items():
                # searching processes only into ignite nodes
                if not image["type"].startswith("ignite"):
                    continue
                image_processes = []
                raw_output = self.exec_in_container(cmd, image)

                # parse ps output
                for line in raw_output.split("\n"):
                    if line:
                        columns = [col for col in line.strip().split(" ") if col]
                        process = {}
                        for column in columns:
                            if column.startswith(job_name_pattern):
                                process["name"] = column[len(job_name_pattern):]
                            elif column.strip().endswith(".py"):
                                process["name"] = 'user_script'
                                process["task"] = None
                            elif column.startswith(task_name_pattern):
                                process["task"] = column[len(task_name_pattern):]
                        process["pid"] = columns[1]
                        process["image"] = name
                        process["host"] = host
                        image_processes.append(process)
                host_images[name] = image_processes
            output[host] = host_images
        return output

    def get_process_list(self, hosts=None):
        """
        Form all processes into list
        """
        if hosts is None:
            hosts = self.ssh.hosts
        processes = []
        for host, images_output in self.get_processes(hosts).items():
            for image_name, image_processes in images_output.items():
                processes = processes + image_processes
        return processes

    def wait_for_process(self, expected,
                         hosts=None,
                         not_expected_processes=None,
                         expected_processes=None,
                         timeout=60,
                         interval=5,
                         stabilization_time=10):
        """
        Waiting until would be found all expected processes
        
        :param expected:                expected processes counts
                                                example: { "chief" : 1, "worker": 10}
        :param hosts:
        :param not_expected_processes:  list of not expected processes 
                                        if we found process then it should be with different PID
        :param expected_processes:      list of expected processes (same name/pids)
        :param timeout:                 timeout of wait
        :param interval:                amount of time between sentry check
        :param stabilization_time:      time for processes stabilizations
        """

        def flat(processes_to_flat):
            processes_list = []
            for host, container in processes_to_flat.items():
                for container_processes in container.values():
                    processes_list += container_processes
            return processes_list

        log_print("Waiting for processes")
        if hosts is None:
            hosts = self.ssh.hosts
        end_time = time() + timeout
        while True:
            if time() > end_time:
                raise AssertionError("Can't wait for processes")
            processes = self.get_processes(hosts)

            if not_expected_processes is not None:
                similar_process = []
                for host, containers in processes.items():
                    for name, con_processes in containers.items():
                        not_exp_con_processes = not_expected_processes[host].get(name, None)
                        if not_exp_con_processes is None:
                            continue
                        not_exp_con_pids = [p['pid'] for p in not_exp_con_processes]
                        for con_process in con_processes:
                            if con_process['pid'] in not_exp_con_pids:
                                similar_process.append(con_process["pid"])

                if similar_process:
                    sleep(interval)
                    log_print("  found similar pids: {}".format(', '.join(similar_process)))
                    continue

            if expected_processes is not None:
                not_found = []
                for host, containers in expected_processes.items():
                    for name, exp_con_processes in containers.items():
                        actual_processes = processes[host].get(name)
                        actual_processes_pids = [p["pid"] for p in actual_processes]
                        not_found_pids = [p['pid'] for p in exp_con_processes if p['pid'] not in actual_processes_pids]
                        not_found += not_found_pids

                if not_found:
                    sleep(interval)
                    log_print("  can't find expected process: ".format(', '.join(not_found)))
                    continue

            for process_name, expected_count in expected.items():
                actual_processes = [actual for actual in flat(processes) if actual["name"] == process_name]
                if len(actual_processes) != expected_count:
                    log_print(" Can't find enough '{}' processes"
                              " Expected: {} Actual: {}".format(process_name,
                                                                expected_count,
                                                                len(actual_processes)))
                    sleep(interval)
                    continue

            log_print(" Wait for process stabilization")
            sleep(stabilization_time)
            previous_pids = sorted([proc["pid"] for proc in flat(processes)])
            now_pids = sorted([proc["pid"] for proc in flat(self.get_processes(hosts))])

            if previous_pids == now_pids:
                return True
            else:
                log_print("  pids changed")

    def kill_process(self, process_name, sign=9, hosts=None):
        """
        kill process into container
        :param process_name:    searched container name
        :param sign:            sign to -9
        :param hosts:           hosts to run
        :return:                container ID
        """
        log_print("Killing process: '{}'".format(process_name))
        if hosts is None:
            hosts = self.ssh.hosts
        for host, images in self.get_processes().items():
            if host not in hosts:
                continue
            for image_name, processes in images.items():
                for process in processes:
                    if process["name"] == process_name:
                        log_print("  {} {} {}".format(process["pid"], host, image_name))
                        cmd = "kill -{} {}".format(sign, process["pid"])
                        self.exec_in_container(cmd, self.running_containers[host][image_name])
                        return
        raise AssertionError("Can't find process to kill by name '{}'".format(process_name))

    def get_executor_output(self, container=None):
        """
        Get container logs and parse tensorflow example output
        Found output:
                TF_CONFIG: text text text
                            text text
                OTHER_VARIABLE: Variable text
        Parsed output:
                {
                    "TF_CONFIG": [ "text text text", "text text"],
                    "OTHER_VARIABLE": ["Variable text"]
                }
        :param container:   container obj
        :return:            list of parsed outputs
        """
        if container is None:
            container = self.find_image_by_type("executor")
        logs = self.get_logs(container)
        result = []

        last_key = "unknown"
        script_result = {}
        logs = logs[0].replace("\t", "").split("\n")
        for line in logs:
            # searching for variable name
            if match("^[A-Z0-9_]+:\s.*", line.replace("_", "")):
                param = line[:line.index(":")]
                if script_result.get(param, False):
                    result.append(script_result)
                    script_result = {}
                last_key = param
                script_result[param] = [line[len(param) + 1:].strip()]
            else:
                script_result[last_key] = script_result.get(last_key, []) + [line.strip()]
        if script_result:
            result.append(script_result)
        return result

    def wait_script_execute(self, run_count=1, container=None, script_name="infinity", timeout=60, interval=5):
        """
        Wait for script execute completion

        :param run_count:   how many times in output found results
        :param container:   container obj
        :param script_name: which script was executed
        :param timeout:     wait timeout
        :param interval:    time wait between checks
        :return:            wait result
        """
        log_print("Waiting for script execution...")
        expected_rows = self.test_scripts[script_name]["result_rows"]
        end_time = time() + timeout
        while True:
            outputs = self.get_executor_output(container=container)
            if len(outputs) == run_count:
                if not [run for run in outputs if run.get("unknown")]:
                    actual_rows = len(outputs[run_count - 1].get("NEXT", []))
                    if actual_rows == expected_rows:
                        log_print(" Executed")
                        return True
                    else:
                        log_print(" Found not completed script result. "
                                  "Expected {} rows. Actual {} rows".format(expected_rows, actual_rows))
                else:
                    log_print(" Found not completed process output")
            else:
                log_print(" Can't find enough outputs count. "
                          "Expected {} Actual {}".format(run_count, len(outputs)))
            if time() > end_time:
                for run in outputs:
                    for key, values in run.items():
                        log_print("Key: {}".format(key))
                        for value in values:
                            log_print("   {}".format(value))
                raise AssertionError("Can't wait for script execution")
            sleep(interval)

    def processes_saved(self, expected_processes=None):
        """
        Compare which processes with the same tasks are stayed in cluster
        For example:
                if we kill worker process then all workers should restart on the same nodes with the same task ID

        :param expected_processes:  expected processes structure
        """
        condition = lambda a_proc, e_proc: a_proc["name"] == e_proc["name"] \
                                           and a_proc["task"] == e_proc["task"]
        self.processes_compare(expected_processes, condition)

    def processes_pids_saved(self, expected_processes):
        """
        Compare which processes stayed in cluster without restart (with same PID)

        :param expected_processes:  expected processes structure
        """
        condition = lambda a_proc, e_proc: a_proc["name"] == e_proc["name"] \
                                           and a_proc["task"] == e_proc["task"] \
                                           and a_proc["pid"] == e_proc["pid"]
        self.processes_compare(expected_processes, condition)

    def processes_compare(self, expected_processes, condition):
        """
        Going throw all porcesses and compare expected processes with actual by custom condition

        :param expected_processes:      actual processes
        :param condition:               compare condition
        """
        actual_processes = self.get_processes()
        for host, images in expected_processes.items():
            for name, image in images.items():
                actual_images = actual_processes.get(host, None)
                assert actual_images is not None, "Can't find images on host '{}'".format(host)
                actual_image = actual_images.get(name, None)
                assert actual_image is not None, "Can't find image '{}' on host '{}'".format(name, host)
                for e_proc in image:
                    found_process = [a_proc for a_proc in actual_image if condition(a_proc, e_proc)]
                    assert found_process, "Can't find process '{} (task {})' on {} in ".format(e_proc["name"],
                                                                                               e_proc["task"],
                                                                                               host, name)

    def get_dir_files(self, container_name=None, path="/tmp", filter_pattern="^tf_us_.+"):
        """
        Collect all files from all

        :param container_name:      target container name
        :param path:                path where need to find free step
        :param filter_pattern:      filter regex for directories names
        :return:                    dirs structure
        """
        result = {}
        for host, images in self.running_containers.items():
            images_data = {}
            for name, image in images.items():
                cmd = "ls {}".format(path)
                output = self.exec_in_container(cmd, image)
                if not output:
                    images_data[name] = []
                else:
                    dir_list = output.split("\n") if '\n' in output else []
                    if filter_pattern:
                        dir_list = [item for item in dir_list if match(filter_pattern, item)]
                    if container_name is not None and name == container_name:
                        return dir_list
                    images_data[name] = dir_list
            result[host] = images_data
        return result
