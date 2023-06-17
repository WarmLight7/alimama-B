#pragma once
#include <fstream>
#include <iostream>

#include "defines.h"
#include "json.hpp"

using json = nlohmann::json;

constexpr int32_t kThreadNum = 20;

struct TestResultConfig {
  std::string test_case_csv;
  int32_t csv_reader_capacity = 1000;
  int32_t thread_num = kThreadNum;
  int32_t timeout_ms = 500;
  int32_t sample_num = 500;
  int32_t qps_limit = 0;
  double accuracy_th = 1.0;
  int32_t final_score_th = 80;
}; 

struct TestMaxQpsConfig {
  std::string test_case_csv;
  int32_t csv_reader_capacity = 1000;
  int32_t qps_baseline = 0;
  int32_t thread_num = kThreadNum;
  int32_t timeout_ms = 500;
  int32_t request_duration_each_iter_sec = 5;
  int32_t max_iter_times = 5;
  double sample_percent_th = 0.99;
  double success_percent_th = 0.99;
  int32_t sample_score_th = 80;
  double qps_step_size_percent = 0.1;
}; 

struct TestCapacityConfig {
  std::string test_case_csv;
  int32_t csv_reader_capacity = 1000;
  int32_t thread_num = 6;
  int32_t timeout_ms = 30;
  int32_t sample_num = 5000;
  int32_t test_duration_sec = 5*60;
  double  success_percent_th = 0.99;
  double  sample_percent_th = 0.99;
  int32_t sample_score_th = 80;
  int32_t M = 20000;
};

struct TestResponseTimeConfig {
  std::string test_case_csv;
  int32_t csv_reader_capacity = 1000;
  int32_t thread_num = kThreadNum;
  int32_t timeout_ms = 500;
  int32_t test_duration_sec = 5*60;
  int32_t max_qps = 0;
  double  success_percent_th = 0.99;
  double  sample_percent_th = 0.99;
  int32_t sample_score_th = 80;
};

struct TestStabilityConfig {
  std::string test_case_csv;
  int32_t csv_reader_capacity = 1000;
  int32_t thread_num = kThreadNum;
  int32_t max_qps = 0;
  double load_percent = 0.6;
  int32_t timeout_ms = 500;
  int32_t test_duration_sec = 5*60;
  double  success_percent_th = 0.99;
  double  sample_percent_th = 0.99;
  int32_t sample_score_th = 80;
};


struct TestConfig {
  int32_t loading_time_sec_timeout;
  TestResultConfig result_cfg;
  TestMaxQpsConfig max_qps_cfg;
  TestCapacityConfig capacity_cfg;
  TestResponseTimeConfig response_time_cfg;
  TestStabilityConfig stability_cfg;
};

void FromJson(const json& j, TestResultConfig& p) {
    p.thread_num = j.at("thread_num").get<int32_t>();
    p.timeout_ms = j.at("timeout_ms").get<int32_t>();
    p.qps_limit = j.at("qps_limit").get<int32_t>();
    p.sample_num = j.at("sample_num").get<int32_t>();
    p.test_case_csv = j.at("test_case_csv").get<std::string>();
    p.csv_reader_capacity = j.at("csv_reader_capacity").get<int32_t>();
    p.accuracy_th = j.at("accuracy_th").get<double>();
    p.final_score_th = j.at("final_score_th").get<int32_t>();
}

void FromJson(const json& j, TestCapacityConfig& p) {
    p.test_case_csv = j.at("test_case_csv").get<std::string>();
    p.csv_reader_capacity = j.at("csv_reader_capacity").get<int32_t>();
    p.thread_num = j.at("thread_num").get<int32_t>();
    p.timeout_ms = j.at("timeout_ms").get<int32_t>();
    p.sample_num = j.at("sample_num").get<int32_t>();
    p.test_duration_sec = j.at("test_duration_sec").get<int32_t>();
    p.sample_percent_th = j.at("sample_percent_th").get<double>();
    p.success_percent_th = j.at("success_percent_th").get<double>();
    p.sample_score_th = j.at("sample_score_th").get<int32_t>();
    p.M = j.at("M").get<int32_t>();
}

void FromJson(const json& j, TestMaxQpsConfig& p) {
    p.thread_num = j.at("thread_num").get<int32_t>();
    p.timeout_ms = j.at("timeout_ms").get<int32_t>();
    p.qps_baseline = j.at("qps_baseline").get<int32_t>();
    p.request_duration_each_iter_sec = j.at("request_duration_each_iter_sec").get<int32_t>();
    p.max_iter_times = j.at("max_iter_times").get<int32_t>();
    p.sample_percent_th = j.at("sample_percent_th").get<double>();
    p.success_percent_th = j.at("success_percent_th").get<double>();
    p.sample_score_th = j.at("sample_score_th").get<int32_t>();
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
    p.sample_percent_th = j.at("sample_percent_th").get<double>();
    p.success_percent_th = j.at("success_percent_th").get<double>();
    p.sample_score_th = j.at("sample_score_th").get<int32_t>();
}

void FromJson(const json& j, TestStabilityConfig& p) {
    p.thread_num = j.at("thread_num").get<int32_t>();
    p.load_percent = j.at("load_percent").get<double>();
    p.timeout_ms = j.at("timeout_ms").get<int32_t>();
    p.test_duration_sec = j.at("test_duration_sec").get<int32_t>();
    p.test_case_csv = j.at("test_case_csv").get<std::string>();
    p.csv_reader_capacity = j.at("csv_reader_capacity").get<int32_t>();
    p.sample_percent_th = j.at("sample_percent_th").get<double>();
    p.success_percent_th = j.at("success_percent_th").get<double>();
    p.sample_score_th = j.at("sample_score_th").get<int32_t>();
}

bool ReadConfigFromFile(const std::string& filename, TestConfig& config) {
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
        FromJson(j["test_result"], config.result_cfg);
    }
    else {
        std::cerr << "Missing or invalid 'test_result' in JSON.\n";
        return false;
    }

    if(j.contains("test_max_qps") && j["test_max_qps"].is_object()) {
        FromJson(j["test_max_qps"], config.max_qps_cfg);
    } else {
        std::cerr << "Missing or invalid 'test_max_qps' in JSON.\n";
        return false;
    }

    if(j.contains("test_stability") && j["test_stability"].is_object()) {
        FromJson(j["test_stability"], config.stability_cfg);
    } else {
        std::cerr << "Missing or invalid 'test_stability' in JSON.\n";
        return false;
    }

    if(j.contains("test_response_time") && j["test_response_time"].is_object()) {
        FromJson(j["test_response_time"], config.response_time_cfg);
    } else {
        std::cerr << "Missing or invalid 'test_response_time' in JSON.\n";
        return false;
    }

    if(j.contains("test_capacity") && j["test_capacity"].is_object()) {
        FromJson(j["test_capacity"], config.capacity_cfg);
    } else {
        std::cerr << "Missing or invalid 'test_capacity' in JSON.\n";
        return false;
    }

    config.loading_time_sec_timeout = j.at("loading_time_sec_timeout").get<int32_t>();
    return true;
}
