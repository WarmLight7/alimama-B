#pragma once

#include <memory>
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>

#include "defines.h"

#include "concurent_queue.h"
#include "csv.h"

struct TestCasePair{
    bool repeat;
    RequestPtr req;
    ResponsePtr response;
};

class TestCaseReader{
public:
    virtual ~TestCaseReader() {};
    virtual void start() = 0;
    virtual void stop() = 0;
    virtual bool pop(TestCasePair& pair) = 0;
};

std::vector<uint64_t> parseUint64List(const std::string& s) {
    std::vector<uint64_t> result;
    std::istringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
        result.push_back(std::stoull(item));
    }
    return result;
}

std::vector<float> parseFloatList(const std::string& s) {
    std::vector<float> result;
    std::istringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
        result.push_back(std::stof(item));
    }
    return result;
}

using CSVDef = csv::CSVReader<6, csv::trim_chars<' '>, csv::no_quote_escape<'\t'> >;
