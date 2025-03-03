#include <nanobind/nanobind.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/bind_map.h>
#include <nanobind/stl/bind_vector.h>
#include <nanobind/stl/unordered_map.h>
#include <cmath>
#include <iostream>
#include <memory>
#include <omp.h>

#include "jobs/jobs.hpp"

namespace nb = nanobind;


using SOCKET_CONF = std::vector<int>;
using JOBSLIST = std::vector<Job>;
using HEATMAP = std::unordered_map<std::string, std::unordered_map<std::string, double>>;
using JOBSHOSTS = std::unordered_map<std::string, std::vector<std::string>>;


void calculate_rem_time(Job& job, const SOCKET_CONF& cluster_socket_conf, const HEATMAP& heatmap, const JOBSHOSTS& jobs_hosts) {
    
    double worst_speedup {job.max_speedup};
    double speedup {};

    bool neighbors_exist {false};
    bool spread_allocation {!(job.socket_conf == cluster_socket_conf)};
    
    for (const auto hostname : job.assigned_hosts) {
        for (const std::string co_job_signature : jobs_hosts.at(hostname)) {
            if (job.get_signature() == co_job_signature) {
                continue;
            }
            
            std::string co_job_name = co_job_signature.substr(co_job_signature.find_first_of(':')+1, co_job_signature.size());
            
            speedup = heatmap.at(job.job_name).at(co_job_name);
            
            if (std::isnan(speedup))
                speedup = job.avg_speedup;
            if (speedup < worst_speedup)
                worst_speedup = speedup;
        }

    }

    if ((job.sim_speedup != worst_speedup) && (neighbors_exist || spread_allocation)) {
        job.remaining_time *= (job.sim_speedup / worst_speedup);
        job.sim_speedup = worst_speedup;
    }

}

std::pair<double, std::pair<JOBSLIST, JOBSLIST>> next_sim_state(
    JOBSLIST& exec_jobs, 
    const JOBSLIST& preload_jobs, 
    double makespan, 
    const SOCKET_CONF& cluster_socket_conf, 
    const HEATMAP& heatmap, 
    const JOBSHOSTS& jobs_hosts) 
{

    int i {};

    #pragma omp parallel for private(i) shared(cluster_socket_conf, heatmap, jobs_hosts)
    for (i = 0; i < exec_jobs.size(); ++i)
        calculate_rem_time(exec_jobs[i], cluster_socket_conf, heatmap, jobs_hosts);
    

    double min_rem_time {std::numeric_limits<double>::max()};
    #pragma omp parallel shared(min_rem_time)
    {
        double local_min {std::numeric_limits<double>::max()};
        
        #pragma omp for
        for (int i = 0; i < exec_jobs.size(); ++i) {
            if (exec_jobs[i].remaining_time < local_min)
                local_min = exec_jobs[i].remaining_time;
        }
        
        #pragma omp critical
        {
            if (local_min < min_rem_time) {
                min_rem_time = local_min;
            }
        }
    }
    
    #pragma omp parallel
    {
        double local_min {std::numeric_limits<double>::max()};

        #pragma omp for
        for (int i = 0; i < preload_jobs.size(); ++i) {
            double showup_time {preload_jobs[i].submit_time - makespan};
            if (showup_time > 0 && showup_time < local_min)
                local_min = showup_time;
        }
        
        #pragma omp critical
        {
            if (local_min < min_rem_time) {
                min_rem_time = local_min;
            }
        }
    }

    JOBSLIST execution_list {};
    JOBSLIST jobs_to_clean {};
    #pragma omp parallel 
    {
        JOBSLIST _exec_list {};
        JOBSLIST _clean_list {};
        
        #pragma omp for
        for (int i = 0; i < exec_jobs.size(); ++i) {
            exec_jobs[i].remaining_time -= min_rem_time;
            if (exec_jobs[i].remaining_time <= 0) {
                _clean_list.push_back(exec_jobs[i]);
            } 
            else {
                _exec_list.push_back(exec_jobs[i]);
            }
        }
        
        #pragma omp critical
        {
            execution_list.insert(execution_list.end(), _exec_list.begin(), _exec_list.end());
            jobs_to_clean.insert(jobs_to_clean.end(), _clean_list.begin(), _clean_list.end());
        }
    }

    // exec_jobs = std::move(execution_list);

    std::pair<JOBSLIST, JOBSLIST> res_lists {execution_list, jobs_to_clean};
    std::pair<double, std::pair<JOBSLIST, JOBSLIST>> result {min_rem_time, res_lists};
    return result;
}

void calculate_rem_times(JOBSLIST& jobs, const SOCKET_CONF& cluster_socket_conf, const HEATMAP& heatmap, const JOBSHOSTS& jobs_hosts) {
    #pragma omp parallel for shared(cluster_socket_conf, heatmap, jobs_hosts)
    for (int i = 0; i < jobs.size(); ++i)
        calculate_rem_time(jobs[i], cluster_socket_conf, heatmap, jobs_hosts);
    
}

NB_MODULE(compengine_utils, m){
    nb::bind_vector<SOCKET_CONF>(m, "SOCKET_CONF");
    nb::bind_vector<JOBSLIST>(m, "JOBSLIST");
    // nb::bind_map<JOBSHOSTS>(m, "JOBSHOSTS");
    m.def("calculate_rem_time", &calculate_rem_time);
    m.def("calculate_rem_times", &calculate_rem_times);
    m.def("next_sim_state", &next_sim_state);
}