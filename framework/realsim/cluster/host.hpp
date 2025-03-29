#ifndef HPP_HOST
#define HPP_HOST

#include <cmath>
#include <iostream>
#include <numeric>

#include <nanobind/nanobind.h>
#include <nanobind/stl/vector.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/unordered_map.h>

/**
 * Core Allocation Polices (CAP for short)
 */
enum class CAP {
    RANDOM, // Random position of cores inside the CoresSet
    SEQUENTIAL, // Sequential position of cores inside the CoresSet
    EXACT, // Exact position of cores inside the CoresSet
};


class CoresSet {

    private:

        std::vector<bool> cores; // false: idle, true: allocated
        int start_core;
        int end_core;

    public:

        CoresSet(int, int);
        std::vector<int> get_idle_cores_ids();
        std::vector<int> get_allocated_cores_ids();
        void allocate_cores(const std::vector<int>&);
        void free_cores(const std::vector<int>&);
        std::vector<int> find_seq_cores(int);
        std::vector<int> find_random_cores(int);
        std::vector<int> find_cores(int, CAP);

};

enum class HostState {
    IDLE,
    ALLOCATED,
    DOWN
};

class Host {

    using SOCKETS_CONF = std::vector<int>;
    using CoreIds = std::vector<int>;

    public:

        HostState state;
        SOCKETS_CONF sockets_conf;
        std::vector<CoresSet> sockets;
        std::unordered_map<std::string, std::vector<CoreIds>> jobsresources;
        
     
        Host(SOCKETS_CONF, int);
        int total_idle_cores_num() const;
        int total_allocated_cores_num() const;
        bool is_request_valid(SOCKETS_CONF, CAP);
        void allocate_job(std::string, SOCKETS_CONF, CAP);
        void free_job(std::string);

};
#endif