#include "jobs.hpp"
#include <nanobind/nanobind.h>
#include <nanobind/stl/bind_vector.h>
#include <nanobind/stl/string.h>
#include <sstream>

namespace nb = nanobind;

using ASSIGNED_HOSTS = std::vector<std::string>;
using SOCKET_CONF = std::vector<int>;
using JOB_TAG = std::vector<double>;


Job::Job(int job_id,
    std::string job_name,
    int num_of_processes,
    ASSIGNED_HOSTS& assigned_hosts,
    double remaining_time,
    double submit_time,
    double waiting_time,
    double wall_time) 
{
    this->job_id = job_id;
    this->job_name = job_name;

    this->num_of_processes = (num_of_processes > 0) ? num_of_processes : 1;
    this->full_socket_nodes = -1;
    this->half_socket_nodes = -1;
    this->assigned_hosts = assigned_hosts;
    this->socket_conf = std::vector<int>();

    this->remaining_time = (remaining_time > 0) ? remaining_time : 0.1;
    this->submit_time = (submit_time > 0) ? submit_time : 0.1;
    this->waiting_time = waiting_time;
    this->wall_time = (wall_time > 0) ? wall_time : 0.1;
    this->start_time = -1.0;
    this->finish_time = -1.0;
    
    this->sim_speedup = 1;
    this->avg_speedup = 1;
    this->max_speedup = 1;
    this->min_speedup = 1;

    this->job_tag.clear();
    this->job_character = static_cast<int>(JobCharacterization::COMPACT);
    this->current_state = static_cast<int>(JobState::PENDING);
}
        
bool Job::operator==(const Job& other) const {
    return job_id == other.job_id && job_name == other.job_name &&
           num_of_processes == other.num_of_processes &&
           assigned_hosts == other.assigned_hosts &&
           remaining_time == other.remaining_time &&
           submit_time == other.submit_time &&
           waiting_time == other.waiting_time &&
           wall_time == other.wall_time;
}

double Job::get_avg_speedup() const {
    return this->avg_speedup;
}

double Job::get_max_speedup() const {
    return this->max_speedup;
}

double Job::get_min_speedup() const {
    return this->min_speedup;
}

Job Job::deepcopy() const {
    Job copy(*this);
    return copy;
}

std::string Job::get_signature() const {
    std::stringstream ss;
    ss << job_id << ":" << job_name;
    return ss.str();
}


NB_MODULE(jobs, m) {
    
    nb::bind_vector<ASSIGNED_HOSTS>(m, "ASSIGNED_HOSTS");
    nb::bind_vector<SOCKET_CONF>(m, "SOCKET_CONF");
    nb::bind_vector<JOB_TAG>(m, "JOB_TAG");

    nb::class_<Job>(m, "Job")
        .def(nb::init<int, const std::string&, int, ASSIGNED_HOSTS&, double, double, double, double>())
        .def("__eq__", &Job::operator==)
        .def("get_avg_speedup", &Job::get_avg_speedup)
        .def("get_max_speedup", &Job::get_max_speedup)
        .def("get_min_speedup", &Job::get_min_speedup)
        .def("deepcopy", &Job::deepcopy)
        .def("get_signature", &Job::get_signature)
        .def_rw("job_id", &Job::job_id)
        .def_rw("job_name", &Job::job_name)
        .def_rw("num_of_processes", &Job::num_of_processes)
        .def_rw("full_socket_nodes", &Job::full_socket_nodes)
        .def_rw("half_socket_nodes", &Job::half_socket_nodes)
        .def_rw("assigned_hosts", &Job::assigned_hosts)
        .def_rw("socket_conf", &Job::socket_conf)
        .def_rw("remaining_time", &Job::remaining_time)
        .def_rw("submit_time", &Job::submit_time)
        .def_rw("waiting_time", &Job::waiting_time)
        .def_rw("wall_time", &Job::wall_time)
        .def_rw("start_time", &Job::start_time)
        .def_rw("finish_time", &Job::finish_time)
        .def_rw("sim_speedup", &Job::sim_speedup)
        .def_rw("avg_speedup", &Job::avg_speedup)
        .def_rw("max_speedup", &Job::max_speedup)
        .def_rw("min_speedup", &Job::min_speedup)
        .def_rw("job_tag", &Job::job_tag)
        .def_rw("job_character", &Job::job_character)
        .def_rw("current_state", &Job::current_state);
}
