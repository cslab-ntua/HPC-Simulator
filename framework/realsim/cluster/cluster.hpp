#ifndef HPP_CLUSTER
#define HPP_CLUSTER

#include <memory>

#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/unordered_map.h>
#include <nanobind/stl/vector.h>

#include "../jobs/jobs.hpp"
#include "host.hpp"

class Cluster {
    public:

        long long int nodes;
        std::vector<int> socket_conf;
        std::vector<int> full_socket_allocation;
        std::vector<int> half_socket_allocation;
        std::vector<int> quarter_socket_allocation;
        
        std::unordered_map<std::string, Host> hosts;
        
        long long int free_cores;
        long long int total_cores;
        long long int idle_cores;

        long long int queue_size;

        std::vector<std::shared_ptr<Job>> waiting_queue;
        std::vector<std::shared_ptr<Job>> execution_list;
        
        long long int id_counter;
        double makespan;

        Cluster(long long int, SOCKET_CONF);
        void setup();
        long long int get_idle_cores() const;
        long long int get_used_cores() const;
        // std::vector<std::vector<std::string>> get_hostname_procs();
        


};

#endif