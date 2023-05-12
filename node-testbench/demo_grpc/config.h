#include <fstream>
#include <iostream>

#include "json.hpp"

using json = nlohmann::json;

constexpr int32_t kThreadNum = 20;

struct TestResultConfig {
  int32_t thread_num = kThreadNum;
  int32_t timeout_ms = 500;
  int32_t qps_limit = 500;
  int32_t request_times = 1000;
  int32_t sample_num = 500;
}; 

struct TestMaxQpsConfig {
  int32_t qps_baseline = 0;
  int32_t thread_num = kThreadNum;
  int32_t timeout_ms = 500;
  int32_t request_duration_each_iter_sec = 5;
  int32_t max_iter_times = 5;
  double success_percent_th = 0.99;
  double qps_step_size_percent = 0.1;
}; 

struct TestStabilityConfig {
  int32_t thread_num = kThreadNum;
  int32_t max_qps = 0;
  double load_percent = 0.6;
  int32_t timeout_ms = 500;
  int32_t test_duration_sec = 5*60;
};


void from_json(const json& j, TestResultConfig& p) {
    p.thread_num = j.at("thread_num").get<int32_t>();
    p.timeout_ms = j.at("timeout_ms").get<int32_t>();
    p.qps_limit = j.at("qps_limit").get<int32_t>();
    p.request_times = j.at("request_times").get<int32_t>();
    p.sample_num = j.at("sample_num").get<int32_t>();
}

void from_json(const json& j, TestMaxQpsConfig& p) {
    p.thread_num = j.at("thread_num").get<int32_t>();
    p.timeout_ms = j.at("timeout_ms").get<int32_t>();
    p.request_duration_each_iter_sec = j.at("request_duration_each_iter_sec").get<int32_t>();
    p.max_iter_times = j.at("max_iter_times").get<int32_t>();
    p.success_percent_th = j.at("success_percent_th").get<double>();
    p.qps_step_size_percent = j.at("qps_step_size_percent").get<double>();
}

void from_json(const json& j, TestStabilityConfig& p) {
    p.thread_num = j.at("thread_num").get<int32_t>();
    p.load_percent = j.at("load_percent").get<double>();
    p.timeout_ms = j.at("timeout_ms").get<int32_t>();
    p.test_duration_sec = j.at("test_duration_sec").get<int32_t>();
}

bool read_config_from_file(const std::string& filename, 
                        TestResultConfig& resultConfig, 
                        TestMaxQpsConfig& maxQpsConfig, 
                        TestStabilityConfig& stabilityConfig) {
    std::ifstream i(filename);
    if(!i.is_open()){
        std::cerr << "Could not open file: " << filename << std::endl;
        return false;
    }
    json j;
    try{
        i >> j;
    }
    catch (json::parse_error& e){
        std::cerr << "JSON parse error: " << e.what() << '\n';
        return false;
    }
    
    if(j.contains("test_result") && j["test_result"].is_object()) {
        from_json(j["test_result"], resultConfig);
    }
    else {
        std::cerr << "Missing or invalid 'test_result' in JSON.\n";
        return false;
    }

    if(j.contains("test_max_qps") && j["test_max_qps"].is_object()) {
        from_json(j["test_max_qps"], maxQpsConfig);
    } else {
        std::cerr << "Missing or invalid 'test_max_qps' in JSON.\n";
        return false;
    }

    if(j.contains("test_stability") && j["test_stability"].is_object()) {
        from_json(j["test_stability"], stabilityConfig);
    } else {
        std::cerr << "Missing or invalid 'test_stability' in JSON.\n";
        return false;
    }

    return true;
}
