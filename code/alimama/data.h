#ifndef DATA_H
#define DATA_H
#include <iostream>
#include <string>
#include <vector>
#include <unordered_map>
#include <bitset>
#include <memory>
#include <shared_mutex>
#include <thread>
#include <sstream>
#include <algorithm>
#include <csv.h>
#include <set>


namespace Data {

class DataProcessor {
protected:
    struct KeywordAdgroup {
        uint32_t price;
        std::bitset<24> timing;
        float vectorx;
        float vectory;
        uint64_t adgroup_id;
        // Overloading the less-than operator ("<")
        bool operator<(const KeywordAdgroup& other) const {
            return price > other.price;
        }
        KeywordAdgroup& operator=(const KeywordAdgroup& other){
            if (this == &other) {
                return *this; // Handling self-assignment
            }

            // Copying values from 'other' to the current object
            timing = other.timing;
            price = other.price;
            vectorx = other.vectorx;
            vectory = other.vectory;
            adgroup_id = other.adgroup_id;

            return *this;
        }
    };
    std::unique_ptr<std::unordered_map<uint64_t, uint32_t>> keywordIDPtr;
    std::unique_ptr<std::vector<std::set<KeywordAdgroup>>> keywordAdgroupPtr;

    std::shared_mutex keywordIDMutex;
    std::shared_mutex adgroupKeywordMutex;
    
    struct Adgroup {
        float score;
        uint32_t price;
        float ctr;
        uint64_t adgroup_id;

        // Overloading the less-than operator ("<")
        bool operator<(const Adgroup& other) const {
            if (std::abs(score - other.score) > 1e-6) {
                return score > other.score;
            }

            if (price != other.price) {
                return price < other.price;
            }

            return adgroup_id > other.adgroup_id;
        }

        // Overloading the assignment operator ("=")
        Adgroup& operator=(const Adgroup& other) {
            if (this == &other) {
                return *this; // Handling self-assignment
            }

            // Copying values from 'other' to the current object
            score = other.score;
            price = other.price;
            ctr = other.ctr;
            adgroup_id = other.adgroup_id;

            return *this;
        }
    };

    
    

    
public:
    const uint32_t keywordCount = 1e6;
    const uint32_t adgroupCount = 5e6;

    DataProcessor() {
        keywordIDPtr = std::make_unique<std::unordered_map<uint64_t, uint32_t>>(keywordCount);
        keywordAdgroupPtr = std::make_unique<std::vector<std::set<KeywordAdgroup>>>(keywordCount);
    }
    static void split2float(const std::string& str, std::pair<float, float>& result) {
        std::stringstream ss(str);
        ss >> result.first;
        ss.ignore(); // 忽略逗号或其他分隔符
        ss >> result.second;
    }

    static std::bitset<24> timings2bitset(std::string& timings){
        timings.erase(std::remove(timings.begin(), timings.end(), ','), timings.end());
        std::bitset<24> timing(timings);
        return timing;
    }

    // bool checkHours(uint64_t adgroup, int hour){
    //     return (*adgroup2timingPtr)[adgroup][23-hour];
    // }

    // void read_csv_map_time(const std::string& csvFile, int startRow, int endRow){
    //     csv::CSVReader<8, csv::trim_chars<>,  csv::no_quote_escape<'\t'> > reader(csvFile);
    //     int currentRow = 0;
    //     uint64_t keyword,adgroup,price,campaign_id,item_id;
    //     uint8_t status;
    //     std::string timingString, itemVectorString;
    //     std::bitset<24> timing;
    //     while (currentRow < endRow && reader.read_row(keyword,adgroup,price,status,timingString,itemVectorString,campaign_id,item_id)){
    //         if (adgroup2timingPtr->find(adgroup) == adgroup2timingPtr->end()) {
    //             (*adgroup2timingPtr)[adgroup] = timings2bitset(timingString);
    //         }
    //     }
    // }


    void read_csv_map_pool(const std::string& csvFile, int startRow, int endRow , int threadNumber, int threadCount) {
        csv::CSVReader<8, csv::trim_chars<>,  csv::no_quote_escape<'\t'> > reader(csvFile);
        int currentRow = 0;
        uint64_t keyword,adgroup,price,campaign_id,item_id;
        uint8_t status;
        std::string timingString, itemVectorString;
        std::pair<float, float> itemVector;
        uint32_t keywordid;
        while (currentRow < endRow && reader.read_row(keyword,adgroup,price,status,timingString,itemVectorString,campaign_id,item_id)){
            if(keyword % threadCount != threadNumber || status == 0){
                currentRow++;
                continue;
            }
            
            {
                std::shared_lock<std::shared_mutex> readLock(keywordIDMutex);
                if (keywordIDPtr->find(keyword) == keywordIDPtr->end()) {
                    readLock.unlock(); 
                    std::unique_lock<std::shared_mutex> writeLock(keywordIDMutex);
                    (*keywordIDPtr)[keyword] = keywordIDPtr->size();
                }
            }
            {
                std::shared_lock<std::shared_mutex> readLock(keywordIDMutex);
                keywordid = (*keywordIDPtr)[keyword];
            }
            {
                std::shared_lock<std::shared_mutex> readLock(adgroupKeywordMutex);
                if (keywordAdgroupPtr->size() <= keywordid) {
                    readLock.unlock(); 
                    std::unique_lock<std::shared_mutex> writeLock(adgroupKeywordMutex);
                    while(keywordAdgroupPtr->size() <= keywordid){
                        (*keywordAdgroupPtr).emplace_back(std::set<KeywordAdgroup>());
                    }
                }
            }
            split2float(itemVectorString, itemVector);
            KeywordAdgroup newKeywordAdgroup;
            newKeywordAdgroup.adgroup_id = adgroup;
            newKeywordAdgroup.price = price;
            newKeywordAdgroup.timing = timings2bitset(timingString);
            newKeywordAdgroup.vectorx = itemVector.first;
            newKeywordAdgroup.vectory = itemVector.second;
            (*keywordAdgroupPtr)[keywordid].insert(newKeywordAdgroup);
            currentRow++;

        }
    }
    void readCsv(const std::string& path) {
        int len = 700000000;
        int startRow = 0;  // Starting row
        int endRow = len;
        int threadCount = 12;
        std::vector<std::thread> threads;
        for (int i = 0; i < threadCount; ++i) {
            threads.emplace_back([this, path, startRow, endRow, i, threadCount]() {
                read_csv_map_pool(path, startRow, endRow, i, threadCount);
            });
        }
        // threads.emplace_back([this, path, startRow, endRow]() {
        //     read_csv_map_time(path, startRow, endRow);
        // });

        // Wait for all threads to complete
        for (auto& thread : threads) {
            thread.join();
        }
    }


};

}
#endif // DATA_H