#ifndef HPP_JOBS
#define HPP_JOBS

#include <nanobind/nanobind.h>
#include <nanobind/stl/bind_vector.h>
#include <nanobind/stl/string.h>

enum JobState {
    PENDING,
    EXECUTING,
    FINISHED,
    FAILED,
    ABORTED
};

enum JobCharacterization {
    COMPACT,
    SPREAD,
    ROBUST,
    FRAIL
};

// using JobState_t = nb::enum_<JobState>;
// using JobCharacterization_t = nb::enum_<JobCharacterization>;

using ASSIGNED_HOSTS = std::vector<std::string>;
using SOCKET_CONF = std::vector<int>;
using JOB_TAG = std::vector<double>;

class Job {
    public:
        // Important identifiers of the job
        int job_id;
        std::string job_name;
        
        // Cores/Nodes resources
        int num_of_processes;
        int full_socket_nodes;
        int half_socket_nodes;
        ASSIGNED_HOSTS assigned_hosts;
        SOCKET_CONF socket_conf;
        
        // Time resources
        double remaining_time;
        double submit_time;
        double waiting_time;
        double wall_time;
        double start_time;
        double finish_time;
        
        // Speedups of job
        double sim_speedup;
        double avg_speedup;
        double max_speedup;
        double min_speedup;
        
        // Job performance tag
        JOB_TAG job_tag;
        
        // Job characterization for schedulers
        int job_character;
        
        // Job's state
        int current_state;

        Job(int job_id,
            std::string job_name,
            int num_of_processes,
            ASSIGNED_HOSTS& assigned_hosts,
            double remaining_time,
            double submit_time,
            double waiting_time,
            double wall_time);
    
        bool operator==(const Job& other) const;
        double get_avg_speedup() const;
        double get_max_speedup() const;
        double get_min_speedup() const;
        Job deepcopy() const;
        std::string get_signature() const;
};


#endif