#include "cluster.hpp"
#include <numeric>
#include <sstream>

namespace nb = nanobind;

SOCKET_CONF inline calculate_socket_allocation(SOCKET_CONF& socket_conf, int divider) {
    try
    {
        SOCKET_CONF new_socket_conf {};
        for (auto cores : socket_conf)
            new_socket_conf.push_back(std::floor(cores / divider));
        return new_socket_conf;
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
}

Cluster::Cluster(long long int nodes, SOCKET_CONF socket_conf): nodes(nodes), socket_conf(socket_conf) {
    full_socket_allocation = socket_conf;
    half_socket_allocation = calculate_socket_allocation(socket_conf, 2);
    quarter_socket_allocation = calculate_socket_allocation(socket_conf, 4);

    long long int cores_per_node {std::accumulate(socket_conf.cbegin(), socket_conf.cend(), 0)};
    for (int i {}; i < nodes; ++i) {
        std::string hostname;
        std::stringstream ss;
        ss << "host" << i;
        ss >> hostname;
        hosts.insert({hostname, Host(socket_conf, i * cores_per_node + 1)});
    }
    
    free_cores = nodes * cores_per_node;
    total_cores = free_cores;
    idle_cores = free_cores;
    
    waiting_queue = {};
    execution_list = {};
    
    id_counter = 0;
    makespan = 0.0;
}

void Cluster::setup() {
    execution_list = {};
}

long long int Cluster::get_idle_cores() const {
    long long int sum {};
    for (const auto& [hostname, host] : hosts)
        sum += host.total_idle_cores_num();
    return sum;
}

long long int Cluster::get_used_cores() const {
    long long int sum {};
    for (const auto& [hostname, host] : hosts)
        sum += host.total_allocated_cores_num();
    return sum;
}

NB_MODULE(cluster, m) {
   nb::class_<Cluster>(m, "Cluster")
        .def(nb::init<long long int, SOCKET_CONF>())
        .def("setup", &Cluster::setup)
        .def("get_idle_cores", &Cluster::get_idle_cores)
        .def("get_used_cores", &Cluster::get_used_cores);
}