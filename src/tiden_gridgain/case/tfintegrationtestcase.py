#!/usr/bin/env python3

from tiden.case.generaltestcase import GeneralTestCase
from ..ignite_tf import IgniteTF
from tiden.xmlconfigbuilder import IgniteTestContext
from tiden.util import log_print
from os.path import join


class TFIntegrationTestCase(GeneralTestCase):

    def __init__(self, config, ssh):
        super().__init__(config, ssh)
        self.ignite = None
        self.contexts["additional"] = IgniteTestContext(config)

    def setup(self):
        for name, test_context in self.contexts.items():
            self.contexts[name].set_client_result_config("client_%s.xml" % name)
            self.contexts[name].set_server_result_config("server_%s.xml" % name)
            self.contexts[name].set_client_template_config("client_%s_temp.xml" % name)
            self.contexts[name].set_server_template_config("server_%s_temp.xml" % name)

            self.contexts[name].build_config()

        super().setup()
        configs = {}
        for name, test_context in self.contexts.items():
            configs[name] = {
                "client_config_name": test_context.client_result_config,
                "client_config_path": join(self.config["rt"]["remote"]["test_module_dir"],
                                           test_context.client_result_config),
                "server_config_name": test_context.server_result_config,
                "server_config_path": join(self.config["rt"]["remote"]["test_module_dir"],
                                           test_context.server_result_config),
            }
        self.ignite = IgniteTF(self.config, self.ssh, configs)
        self.ignite.remove_all_images_and_container()
        self.ignite.load_images()

    def teardown(self):
        self.ignite.remove_all_images_and_container()
        super().teardown()

    def print_processes(self):
        """
        Print all python processes in all container
        """
        processes = self.ignite.get_processes()
        log_print("Processes map:")
        for host, images in processes.items():
            log_print("  Host {}".format(host))
            for name, processes in images.items():
                log_print("    container: {}".format(name))
                log_print("    processes:")
                for process in processes:
                    log_print("      - {} task:{} {}".format(process["pid"], process["task"], process["name"]))
                if not processes:
                    log_print("      [empty]")
                log_print("")
            log_print("")

    def print_containers_info(self):
        """
        Print information about all running container
        """
        log_print("Containers map:")
        for host, images in self.ignite.running_containers.items():
            log_print("  Host '{}'".format(host))
            for name, image in images.items():
                log_print("    container '{}' {} {}...".format(name, image["type"], image["id"][:5]))

    def filter_by_image(self, processes, *images):
        """
        Filter processes in specific image
        """
        cond = lambda host, name, _processes: name in images
        return self._filter(processes, cond)

    def exclude_by_process(self, processes, *processes_names):
        """
        Exclude image by process name
        """
        cond = lambda host, name, _processes: [_proc for _proc in _processes if _proc["name"] in processes_names]
        return self._filter(processes, cond)

    def _filter(self, processes, condition):
        new_processes = {}
        for host, images in processes.items():
            new_images = {}
            for name, processes in images.items():
                if condition(host, name, processes):
                    continue
                else:
                    new_images[name] = processes
            new_processes[host] = new_images
        return new_processes

    def find_empty_processes_image(self, processes):
        for images in processes.values():
            for name, processes_list in images.items():
                if len(processes_list) == 0:
                    return name

    def find_container_name_by_process(self, processes=None, process_name=None):
        if processes is None:
            processes = self.ignite.get_processes()
        condition = lambda i_processes: [process for process in i_processes if process["name"] == process_name]
        processes = self.ignite.find(processes, condition)
        return processes[0]["image"]


def print_fail_stat(func):
    """
    Print current containers info and processes info on test fail
    """
    def exec_print(*args):
        try:
            result = func(*args)
        except:
            args[0].print_containers_info()
            args[0].print_processes()
            raise
        return result

    return exec_print