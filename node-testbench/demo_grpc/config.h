#pragma once
#include <fstream>
#include <iostream>

#define BOOST_LOG_DYN_LINK 1
#include <boost/log/core.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
namespace logging = boost::log;

#include "json.hpp"

using json = nlohmann::json;

constexpr int32_t kThreadNum = 20;

struct TestResultConfig {
  std::string test_case_csv;
  int32_t csv_reader_capacity = 1000;
  int32_t thread_num = kThreadNum;
  int32_t timeout_ms = 500;
  int32_t qps_limit = 500;
  int32_t request_times = 1000;
  int32_t sample_num = 500;
}; 

struct TestMaxQpsConfig {
  std::string test_case_csv;
  int32_t csv_reader_capacity = 1000;
  int32_t qps_baseline = 0;
  int32_t thread_num = kThreadNum;
  int32_t timeout_ms = 500;
  int32_t request_duration_each_iter_sec = 5;
  int32_t max_iter_times = 5;
  double success_percent_th = 0.99;
  double qps_step_size_percent = 0.1;
}; 

struct TestResponseTimeConfig {
  std::string test_case_csv;
  int32_t csv_reader_capacity = 1000;
  int32_t thread_num = kThreadNum;
  int32_t timeout_ms = 500;
  int32_t test_duration_sec = 5*60;
  int32_t max_qps = 0;
};

struct TestStabilityConfig {
  std::string test_case_csv;
  int32_t csv_reader_capacity = 1000;
  int32_t thread_num = kThreadNum;
  int32_t max_qps = 0;
  double load_percent = 0.6;
  int32_t timeout_ms = 500;
  int32_t test_duration_sec = 5*60;
};


void FromJson(const json& j, TestResultConfig& p) {
    p.thread_num = j.at("thread_num").get<int32_t>();
    p.timeout_ms = j.at("timeout_ms").get<int32_t>();
    p.qps_limit = j.at("qps_limit").get<int32_t>();
    p.request_times = j.at("request_times").get<int32_t>();
    p.sample_num = j.at("sample_num").get<int32_t>();
    p.test_case_csv = j.at("test_case_csv").get<std::string>();
    p.csv_reader_capacity = j.at("csv_reader_capacity").get<int32_t>();
}

void FromJson(const json& j, TestMaxQpsConfig& p) {
    p.thread_num = j.at("thread_num").get<int32_t>();
    p.timeout_ms = j.at("timeout_ms").get<int32_t>();
    p.request_duration_each_iter_sec = j.at("request_duration_each_iter_sec").get<int32_t>();
    p.max_iter_times = j.at("max_iter_times").get<int32_t>();
    p.success_percent_th = j.at("success_percent_th").get<double>();
    p.qps_step_size_percent = j.at("qps_step_size_percent").get<double>();
    p.test_case_csv = j.at("test_case_csv").get<std::string>();
    p.csv_reader_capacity = j.at("csv_reader_capacity").get<int32_t>();
}

void FromJson(const json& j, TestResponseTimeConfig& p) {
    p.thread_num = j.at("thread_num").get<int32_t>();
    p.timeout_ms = j.at("timeout_ms").get<int32_t>();
    p.test_duration_sec = j.at("test_duration_sec").get<int32_t>();
    p.test_case_csv = j.at("test_case_csv").get<std::string>();
    p.csv_reader_capacity = j.at("csv_reader_capacity").get<int32_t>();
}

void FromJson(const json& j, TestStabilityConfig& p) {
    p.thread_num = j.at("thread_num").get<int32_t>();
    p.load_percent = j.at("load_percent").get<double>();
    p.timeout_ms = j.at("timeout_ms").get<int32_t>();
    p.test_duration_sec = j.at("test_duration_sec").get<int32_t>();
    p.test_case_csv = j.at("test_case_csv").get<std::string>();
    p.csv_reader_capacity = j.at("csv_reader_capacity").get<int32_t>();
}

bool ReadConfigFromFile(const std::string& filename, 
                        TestResultConfig& result_config, 
                        TestMaxQpsConfig& max_qps_config, 
                        TestStabilityConfig& stability_config,
                        TestResponseTimeConfig& response_config) {
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
        FromJson(j["test_result"], result_config);
    }
    else {
        std::cerr << "Missing or invalid 'test_result' in JSON.\n";
        return false;
    }

    if(j.contains("test_max_qps") && j["test_max_qps"].is_object()) {
        FromJson(j["test_max_qps"], max_qps_config);
    } else {
        std::cerr << "Missing or invalid 'test_max_qps' in JSON.\n";
        return false;
    }

    if(j.contains("test_stability") && j["test_stability"].is_object()) {
        FromJson(j["test_stability"], stability_config);
    } else {
        std::cerr << "Missing or invalid 'test_stability' in JSON.\n";
        return false;
    }

    if(j.contains("test_response_time") && j["test_response_time"].is_object()) {
        FromJson(j["test_response_time"], response_config);
    } else {
        std::cerr << "Missing or invalid 'test_response_time' in JSON.\n";
        return false;
    }

    return true;
}
