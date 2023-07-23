#ifndef SORT_H
#define SORT_H

#ifdef BAZEL_BUILD
#include "examples/protos/alimama.grpc.pb.h"
#else
#include "alimama.grpc.pb.h"
#endif
using alimama::proto::Request;
using alimama::proto::Response;

#include <data.h>
#include <unordered_set>

namespace Sort {


class SortMethod: public Data::DataProcessor{
public:
    SortMethod() : DataProcessor() {}
    static float getCtr(const std::pair<float, float>& A, const std::pair<float, float>& B) {
        return A.first * B.first + A.second * B.second + 0.000001f;
    }
    void getListHeapsort(const Request* request, Response* response){
        std::pair<float, float> userVector = std::make_pair(request->context_vector()[0], request->context_vector()[1]);
        uint32_t hour = request->hour();
        uint32_t topn = request->topn();
        
        int keywordLength =  request->keywords().size();
        float ctr;
        uint32_t price;
        float score;
        std::vector<Adgroup> adGroupPQ;
        for(const auto & userKeyword : request->keywords()){
            auto keywordIt = keywordIDPtr->find(userKeyword);
            if (keywordIt == keywordIDPtr->end()) {
                continue;
            }
            const auto& keywordid = keywordIt->second;
            int index = 0;
            for(const auto& adgroup: (*keywordAdgroupidPtr)[keywordid]){
                if(checkHours(adgroup, hour)){
                    ctr = getCtr(userVector, (*keywordAdgroup2vectorPtr)[keywordid][index]);
                    price = (*keywordAdgroup2pricePtr)[keywordid][index];
                    score = ctr*price;
                    Adgroup nowAdgroup;
                    nowAdgroup.score = score;
                    nowAdgroup.adgroup_id = adgroup;
                    nowAdgroup.ctr = ctr;
                    nowAdgroup.price = price;
                    adGroupPQ.emplace_back(nowAdgroup);
                }
                index++;
            }
        }
        std::unordered_set<uint64_t> adgroupset;
        std::make_heap(adGroupPQ.begin(), adGroupPQ.end());
        int index = 0;
        while(!adGroupPQ.empty() && index < topn){
            if(adgroupset.find(adGroupPQ[0].adgroup_id) == adgroupset.end()){
                
                response->add_adgroup_ids(adGroupPQ[0].adgroup_id);
                if(index != 0){
                    response->add_prices(adGroupPQ[0].score/ctr+0.5);
                }
                ctr = adGroupPQ[0].ctr;
                price = adGroupPQ[0].price;
                adgroupset.insert(adGroupPQ[0].adgroup_id);
                index++;
            }
            std::pop_heap(adGroupPQ.begin(), adGroupPQ.end());
            adGroupPQ.pop_back();
        }
        if(index < topn){
            response->add_prices(price);
        }
        else{
            response->add_prices(adGroupPQ[0].score/ctr+0.5);
        }
    }

    void getListBucketsort(const Request* request, Response* response){
        std::pair<float, float> userVector = std::make_pair(request->context_vector()[0], request->context_vector()[1]);
        uint32_t hour = request->hour();
        uint32_t topn = request->topn();
        
        int keywordLength =  request->keywords().size();
        float ctr;
        uint32_t price;
        float score;
        std::vector<std::vector<Adgroup>> adGroupBucket(65536);
        for(const auto & userKeyword : request->keywords()){
            auto keywordIt = keywordIDPtr->find(userKeyword);
            if (keywordIt == keywordIDPtr->end()) {
                continue;
            }
            const auto& keywordid = keywordIt->second;
            int index = 0;
            for(const auto& adgroup: (*keywordAdgroupidPtr)[keywordid]){
                if(checkHours(adgroup, hour)){
                    Adgroup nowAdgroup;
                    nowAdgroup.ctr = getCtr(userVector, (*keywordAdgroup2vectorPtr)[keywordid][index]);
                    if(ctr < 0){
                        continue;
                    }
                    nowAdgroup.price = (*keywordAdgroup2pricePtr)[keywordid][index];
                    nowAdgroup.score = nowAdgroup.ctr * nowAdgroup.price;
                    nowAdgroup.adgroup_id = adgroup;
                    if(nowAdgroup.score>65535){
                        continue;
                    }
                    adGroupBucket[(int)(nowAdgroup.score)].emplace_back(nowAdgroup);
                }
                index++;
            }
        }
        
        std::unordered_set<uint64_t> adgroupset;
        int index = 0, i = 0 , j = 0;
        for(i = 65535; i >= 0 && index < topn+1; i--){
            if(adGroupBucket[i].size() == 0){
                continue;
            }
            else if(adGroupBucket[i].size() > 1){
                std::sort(adGroupBucket[i].begin(), adGroupBucket[i].end());
            }
            
            for(j = 0 ; j < adGroupBucket[i].size() && index < topn+1 ; j++){
                if(adgroupset.find(adGroupBucket[i][j].adgroup_id) == adgroupset.end()){
                    if(index != topn){
                        response->add_adgroup_ids(adGroupBucket[i][j].adgroup_id);
                    }
                    if(index != 0){
                        response->add_prices(adGroupBucket[i][j].score/ctr+0.5);
                    }
                    ctr = adGroupBucket[i][j].ctr;
                    price = adGroupBucket[i][j].price;
                    adgroupset.insert(adGroupBucket[i][j].adgroup_id);
                    index++;
                }
            }
            if(index > topn){
                break;
            }
        }
        if(index < topn+1){
            response->add_prices(price);
        }
    }

    void getListQuicksort(const Request* request, Response* response){
        std::pair<float, float> userVector = std::make_pair(request->context_vector()[0], request->context_vector()[1]);
        uint32_t hour = request->hour();
        uint32_t topn = request->topn();
        
        int keywordLength =  request->keywords().size();
        float ctr;
        uint32_t price;
        float score;
        std::vector<Adgroup> adGroupPQ;
        std::unordered_map<uint64_t,uint32_t> adgroupmap;
        for(const auto & userKeyword : request->keywords()){
            auto keywordIt = keywordIDPtr->find(userKeyword);
            if (keywordIt == keywordIDPtr->end()) {
                continue;
            }
            int index = 0;
            const auto& keywordid = keywordIt->second;
            for(const auto& adgroup: (*keywordAdgroupidPtr)[keywordid]){
                if(checkHours(adgroup, hour)){
                    Adgroup nowAdgroup;
                    nowAdgroup.ctr = getCtr(userVector, (*keywordAdgroup2vectorPtr)[keywordid][index]);
                    nowAdgroup.price = (*keywordAdgroup2pricePtr)[keywordid][index];
                    nowAdgroup.score = nowAdgroup.ctr * nowAdgroup.price;
                    nowAdgroup.adgroup_id = adgroup;
                    if(adgroupmap.find(adgroup) == adgroupmap.end()){
                        adgroupmap[adgroup] = adGroupPQ.size();
                        adGroupPQ.emplace_back(nowAdgroup);
                    }
                    else{
                        size_t adgroupIndex = adgroupmap[adgroup];
                        if (nowAdgroup < adGroupPQ[adgroupIndex]) {
                            adGroupPQ[adgroupIndex].score = nowAdgroup.score;
                            adGroupPQ[adgroupIndex].ctr = nowAdgroup.ctr;
                            adGroupPQ[adgroupIndex].price = nowAdgroup.price;
                        }
                    }
                }
                index++;
            }
        }
        size_t k = std::min((size_t)topn+1, (size_t)adGroupPQ.size());
        std::nth_element(adGroupPQ.begin(), adGroupPQ.begin() + k - 1, adGroupPQ.end());
        std::sort(adGroupPQ.begin(), adGroupPQ.begin() + k);
        int index = 0;
        int i = 0;
        for(i = 0 ; i < adGroupPQ.size() && index < topn ; i++){
            response->add_adgroup_ids(adGroupPQ[i].adgroup_id);
            if(index != 0){
                response->add_prices(adGroupPQ[i].score/ctr+0.5);
            }
            ctr = adGroupPQ[i].ctr;
            price = adGroupPQ[i].price;
            index++;
            
        }
        if(index < topn){
            response->add_prices(price);
        }
        else{
            response->add_prices(adGroupPQ[i].score/ctr+0.5);
        }
    }
};

}
#endif // SORT_H