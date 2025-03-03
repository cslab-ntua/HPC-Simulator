#include <set>
#include <string>
#include <vector>
#include <unordered_map>

enum STATES {
    IDLE,
    ALLOCATED,
    DOWN
};

using SOCKET_CONF = std::vector<int>;
// using ProcSet = std::set<int>;

class CoresSet {
    public:
        std::set<int> cset;

        CoresSet(int start, int end) {
            for (int i = start; i <= end; ++i) {
                cset.insert(i);
            }
        }
        
        void free_cores(std::vector<int>& processors) {
            cset.insert(processors.begin(), processors.end());
        }
        
        std::vector<int> occupy_cores(int num_cores) {
            if (num_cores <= cset.size()) {
                std::vector<int> cores {};
                for (auto it = cset.begin(); it != cset.end() && cores.size() < num_cores; ++it) {
                    cores.push_back(*it);
                    cset.erase(it);
                }
                return cores;
            }
            else {
                return {};
            }
        }

};

class Host {
    public:

        enum STATES state;
        SOCKET_CONF socket_conf;
        std::vector<ProcSet> sockets;
        std::unordered_map<std::string, ProcSet> jobs;
        
        Host(SOCKET_CONF socket_conf, int first_core_id) : 
            state(IDLE), 
            socket_conf(socket_conf)
        {
            // Define sockets
            int _count {first_core_id};
            for (int cores : socket_conf) {
                sockets.push_back(ProcSet(_count, _count + cores - 1));
                _count += cores;
            }
        }
        
        int get_idle_cores_num() const {
            int _sum = 0;
            for (const auto& pset : sockets) {
                _sum += pset.size();
            }
            return _sum;
        }
        
};