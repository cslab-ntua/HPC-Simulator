#include "host.hpp"

namespace nb = nanobind;

CoresSet::CoresSet(int start, int end): 
    start_core(start), end_core(end) {
        std::vector<bool> init_cores(end-start+1, false);
        cores = init_cores;
}

/**
 * @brief Retrieves the IDs of idle (unallocated) cores from the CoresSet.
 *
 * This method iterates through the internal `cores` vector and collects
 * the indices of the cores that are currently idle (i.e., those that are
 * marked as false). The resulting vector of core IDs is returned.
 *
 * @return A vector of integers representing the IDs of idle cores.
 *         Each integer corresponds to the index of an idle core in
 *         the `cores` vector.
 */
std::vector<int> CoresSet::get_idle_cores_ids() {
    std::vector<int> core_ids {};
    for (int i {0}; i < cores.size(); ++i) {
        if (!cores[i]) {
            core_ids.push_back(i);
        }
    }
    return core_ids;
}

/**
 * @brief Retrieves the IDs of allocated cores from the CoresSet.
 *
 * This method iterates through the internal `cores` vector and collects
 * the indices of the cores that are currently allocated (i.e., those that
 * are marked as true). The resulting vector of core IDs is returned.
 *
 * @return A vector of integers representing the IDs of allocated cores.
 *         Each integer corresponds to the index of an allocated core in
 *         the `cores` vector.
 */
std::vector<int> CoresSet::get_allocated_cores_ids() {
    std::vector<int> core_ids {};
    for (int i {0}; i < cores.size(); ++i) {
        if (cores[i]) {
            core_ids.push_back(i);
        }
    }
    return core_ids;
}
        
/**
 * @brief Allocates a set of cores as available.
 *
 * This method marks the specified core IDs as allocated, indicating they are
 * currently in use.  It updates the `cores` map to reflect this allocation.
 *
 * @param core_ids A vector of integer core IDs to mark as allocated.
 */
void CoresSet::allocate_cores(const std::vector<int>& core_ids) {
    for (auto core_id : core_ids) {
        cores[core_id] = true;
    }
}

/**
 * @brief Frees the specified cores by setting their status to false in the CoresSet.
 *
 * This function iterates through a vector of core IDs and sets the status of each core
 * to false in the `cores` data structure.  It includes error handling to catch potential
 * issues if a core ID is out of the valid range.
 *
 * @param core_ids A vector of core IDs to free.
*/
void CoresSet::free_cores(const std::vector<int>& core_ids) {
    for (auto id : core_ids) {
        try {
            cores[id - start_core] = false;
        }
        catch(const std::exception& e)
        {
            std::cerr << "The core id is lower or higher than the core id ranges in this CoresSet\n";
            std::cerr << e.what() << '\n';
        }
    }
}
        
/**
 * @brief Finds a set of sequential core IDs based on the requested number of cores.
 *
 * This method determines a set of core IDs that can be used for a given number of cores.
 * It iterates through potential divisions of the core vector, checking if enough cores
 * are available for each division.  If a division is found to have sufficient cores,
 * it returns the IDs for that division. If no division has enough cores, it returns an empty vector.
 *
 * @param num_cores The number of cores to allocate.
 * @return A vector of integers representing the core IDs.  Returns an empty vector if no
 *         sufficient number of sequential cores can be found.
 */
std::vector<int> CoresSet::find_seq_cores(int num_cores) {

    // For each division check if the number of requested cores suffices
    std::vector<int> core_ids {};
    int seq_cores {0};
    for (int i {0}; i < cores.size(); ++i) {
        if (cores[i]) {
            seq_cores = 0;
            core_ids = {};
        }
        else {
            ++seq_cores;
            core_ids.push_back(i);
            if (seq_cores >= num_cores)
                return core_ids;
        }
    }
    
    return core_ids;

}
        
/**
 * @brief Finds available cores within the `CoresSet` by randomly selecting them.
 *
 * This method iterates through the `CoresSet` and identifies idle cores (those with a value of `false`).
 * It randomly selects `num_cores` idle cores and returns their indices within the `CoresSet`.
 * If fewer than `num_cores` idle cores are available, an empty vector is returned.
 *
 * @param num_cores The number of idle cores to select.
 * @return A vector of integers representing the indices of the selected idle cores within the `CoresSet`.
 *         Returns an empty vector if fewer than `num_cores` idle cores are available.
 */
std::vector<int> CoresSet::find_random_cores(int num_cores) {
   std::vector<int> core_ids {};
   int core_count {0};
   for (int i {0}; i < cores.size() && core_count < num_cores; ++i) {
       if (cores[i] == false) {
          core_ids.push_back(i);
           ++core_count;
       }
   }
   
   if (core_count < num_cores) return {};
   else return core_ids;

}
        
/**
 * @brief Finds available cores based on a specified allocation policy.
 *
 * This function determines a set of available cores to use, based on the
 * provided allocation policy.  The default policy is RANDOM, which assigns
 * cores found in random order.  The SEQUENTIAL policy assigns cores in a
 * sequential order.
 *
 * @param num_cores The number of cores to allocate.
 * @param policy The allocation policy to use.  Defaults to CAP::RANDOM.
 * @return A vector of integers representing the assigned core IDs.
 *         The core IDs are assigned based on the selected policy.
 */
std::vector<int> CoresSet::find_cores(int num_cores, CAP policy = CAP::RANDOM) {
    switch (policy) {
        case CAP::SEQUENTIAL:
            return find_seq_cores(num_cores);
            break;
        case CAP::EXACT:
        //  return find_exact_cores(num_cores);
            return  {};
            break;
        default:
            return find_random_cores(num_cores);
    }
}

Host::Host(SOCKETS_CONF socket_conf, int first_core_id) : 
    state(HostState::IDLE), 
    sockets_conf(socket_conf)
        {
            // Define sockets
            int _count {first_core_id};
            for (int cores : socket_conf) {
                sockets.push_back(CoresSet(_count, _count + cores - 1));
                _count += cores;
            }
        }
        
int Host::total_idle_cores_num() const {
    int sum {0};
    for (auto socket : sockets)
        sum += socket.get_idle_cores_ids().size();
    return sum;
}
        
int Host::total_allocated_cores_num() const {
    int sum {0};
    for (auto socket : sockets)
        sum += socket.get_allocated_cores_ids().size();
    return sum;
}
        
/**
 * Checks if a requested sockets configuration is valid for the host.
 *
 * This method validates a given sockets configuration request against the
 * current state of the host and its available socket resources. It ensures
 * that the requested number of cores for each socket type does not exceed
 * the host's available resources, considering the specified policy.
 *
 * @param req_sockets_conf A vector of integers representing the requested
 *                         number of cores for each socket.
 * @param policy The policy to consider when checking if the request can be met.
 *               Defaults to CAP::RANDOM.
 * @return True if the request is valid, False otherwise.
 */
bool Host::is_request_valid(SOCKETS_CONF req_sockets_conf, CAP policy = CAP::RANDOM) {
    
    // Check if the requested socket configuration is valid
    for (int i {0}; i < req_sockets_conf.size(); ++i)
        if (req_sockets_conf[i] > sockets_conf[i])
            return false;

    // If the host is free then immediately return true
    if (state == HostState::IDLE) {
        return true;
    }

    // Check if under the specific policy the request can be met
    for (int i {0}; i < req_sockets_conf.size(); ++i) {
        int req_cores {req_sockets_conf[i]};
        if (req_cores > sockets[i].find_cores(req_cores, policy).size())
           return false;
    }
    
    return true;
}
        
/**
 * @brief Allocates resources (cores) for a given job on the host.
 *
 * This method allocates cores from the available pool to a specified job,
 * based on the requested socket configuration and a specified policy.
 *
 * @param job_name A string representing the name of the job for which resources are being allocated.
 * @param req_socket_conf A vector of integers representing the number of cores requested for each socket.
 * @param policy The policy to use when finding available cores (default: CAP::RANDOM).
 *               This determines how cores are selected from the available pool.
 *
 * @return void
 *
 * @note This method updates the `jobsresources` map with the allocated resources
 *       and sets the `state` of the host to `HostState::ALLOCATED`.
 *
 * @see HostState
 * @see CAP
 * @see sockets
 * @see jobsresources
 */
void Host::allocate_job(std::string job_signature, SOCKETS_CONF req_socket_conf, CAP policy = CAP::RANDOM) {
    std::vector<CoreIds> resources {};
    int req_cores {};
    CoreIds core_ids {};
    for (int i {0}; i < req_socket_conf.size(); ++i) {
        // Get requested cores for this socket
        req_cores = req_socket_conf[i];
        // Find the available cores for this socket under the specific policy
        core_ids = sockets[i].find_cores(req_cores, policy);
        // Allocate the found cores to this socket
        sockets[i].allocate_cores(core_ids);
        // Add the found cores to the resources list
        resources.push_back(core_ids);
    }
    
    // Update the jobresources record of the host
    jobsresources.insert({job_signature, resources});
    
    // Update the state of the host as allocated
    state = HostState::ALLOCATED;
}
        
/**
 * @brief Frees a job and its associated resources from the host.
 *
 * This function removes a job from the host's active jobs and releases the
 * cores allocated to it. It also checks if the host is now completely idle.
 *
 * @param job_name The name of the job to free.
 *
 * @throws std::exception If an error occurs during the process (e.g., invalid job name).
 */
void Host::free_job(std::string job_signature) {
    try
    {
        // Free up the allocated cores for each socket
        for (int i {0}; i < jobsresources[job_signature].size(); ++i)
        {
            CoreIds allocated_cores_ids = jobsresources[job_signature][i];
            sockets[i].free_cores(allocated_cores_ids);
        }
        
        // Check if the host is completely free
        if (std::accumulate(sockets_conf.begin(), sockets_conf.end(), 0) == total_idle_cores_num())
            state = HostState::IDLE;
        
        // Remove job from the jobsresources record
        jobsresources.erase(job_signature);
    }
    catch(const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
    
}
        
NB_MODULE(host, m) {
    nb::class_<Host>(m, "Host")
        .def(nb::init<std::vector<int>, int>())
        .def_ro("state", &Host::state)
        .def_ro("sockets_conf",&Host::sockets_conf)
        .def_ro("sockets", &Host::sockets)
        .def_rw("jobsresources", &Host::jobsresources)
        .def_prop_ro("total_idle_cores_num", &Host::total_idle_cores_num)
        .def_prop_ro("total_allocated_cores_num", &Host::total_allocated_cores_num)
        .def("is_request_valid", &Host::is_request_valid)
        .def("allocate_job", &Host::allocate_job);
    
    nb::class_<CoresSet>(m, "CoresSet")
        .def(nb::init<int, int>())
        .def_prop_ro("idle_cores_ids", &CoresSet::get_idle_cores_ids)
        .def_prop_ro("allocated_cores_ids", &CoresSet::get_allocated_cores_ids)
        .def("allocate_cores", &CoresSet::allocate_cores)
        .def("free_cores", &CoresSet::free_cores)
        .def("find_seq_cores", &CoresSet::find_seq_cores)
        .def("find_random_cores", &CoresSet::find_random_cores)
        .def("find_cores", &CoresSet::find_cores);
    
    nb::enum_<HostState>(m, "HostState")
        .value("IDLE", HostState::IDLE)
        .value("ALLOCATED", HostState::ALLOCATED)
        .value("DOWN", HostState::DOWN);

    nb::enum_<CAP>(m, "CAP")
        .value("RANDOM", CAP::RANDOM)
        .value("SEQUENTIAL", CAP::SEQUENTIAL);
}
