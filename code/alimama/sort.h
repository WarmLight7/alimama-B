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
#include <queue>

namespace Sort {


class SortMethod: public Data::DataProcessor{
public:
    SortMethod() : DataProcessor() {}
    static float getCtr(const float& Ax, const float& Ay, const float& Bx, const float& By) {
        return Ax * Bx + Ay * By + 0.000001f;
    }

    void getListHeapsort(const Request* request, Response* response){
        float uservectorx = request->context_vector()[0];
        float uservectory = request->context_vector()[1];
        uint32_t hour = request->hour();
        uint32_t topn = request->topn();
        
        int keywordLength =  request->keywords().size();
        float ctr;
        uint32_t price;
        float score;
        int count = 10;
        std::priority_queue<Adgroup> adGroupPQ;
        for(const auto & userKeyword : request->keywords()){
            auto keywordIt = keywordIDPtr->find(userKeyword);
            if (keywordIt == keywordIDPtr->end()) {
                continue;
            }
            const auto& keywordid = keywordIt->second;
            for(const auto& adgroup: (*keywordAdgroupPtr)[keywordid]){
                if(adgroup.timing[23-hour]){
                    Adgroup nowAdgroup;
                    nowAdgroup.adgroup_id = adgroup.adgroup_id;
                    nowAdgroup.ctr = getCtr(uservectorx, uservectory, adgroup.vectorx, adgroup.vectory);;
                    nowAdgroup.price = adgroup.price;
                    nowAdgroup.score = nowAdgroup.ctr*nowAdgroup.price;
                    if(adGroupPQ.size() >= topn + count && adGroupPQ.top().score > nowAdgroup.price){
                        break;
                    }
                    if(adGroupPQ.size() < topn+count){
                        adGroupPQ.push(nowAdgroup);
                    }
                    else if(nowAdgroup < adGroupPQ.top()){
                        adGroupPQ.pop();
                        adGroupPQ.push(nowAdgroup);
                    }
                }
            }
        }

        std::unordered_set<uint64_t> adgroupset;
        std::vector<Adgroup> adgroupvector;
        int PQsize = adGroupPQ.size();
        while(!adGroupPQ.empty()){
            adgroupset.insert(adGroupPQ.top().adgroup_id);
            adgroupvector.emplace_back(adGroupPQ.top());
            //std::cout << adGroupPQ.top().score << " " << adGroupPQ.top().adgroup_id << std::endl;
            adGroupPQ.pop();
        }
        //std::cout << std::endl;
        if(adgroupset.size() <= topn + 1){
            //todo
        }
        int index = 0;
        for(int i = adgroupvector.size() - 1 ; i >= 0  && index  < topn+1 ; i--){
            if(adgroupset.find(adgroupvector[i].adgroup_id) == adgroupset.end()){
                continue;
            }
            if(index!=topn){
                response->add_adgroup_ids(adgroupvector[i].adgroup_id);
            }
            if(index != 0){
                response->add_prices(adgroupvector[i].score/ctr+0.5);
            }
            ctr = adgroupvector[i].ctr;
            price = adgroupvector[i].price;
            adgroupset.erase(adgroupvector[i].adgroup_id);
            index++;
        }
        if(index < topn+1){
            response->add_prices(price);
        }

        
    }

    // void getListBucketsort(const Request* request, Response* response){
    //     std::pair<float, float> userVector = std::make_pair(request->context_vector()[0], request->context_vector()[1]);
    //     uint32_t hour = request->hour();
    //     uint32_t topn = request->topn();
        
    //     int keywordLength =  request->keywords().size();
    //     float ctr;
    //     uint32_t price;
    //     float score;
    //     std::vector<std::vector<Adgroup>> adGroupBucket(65536);
    //     for(const auto & userKeyword : request->keywords()){
    //         auto keywordIt = keywordIDPtr->find(userKeyword);
    //         if (keywordIt == keywordIDPtr->end()) {
    //             continue;
    //         }
    //         const auto& keywordid = keywordIt->second;
    //         int index = 0;
    //         for(const auto& adgroup: (*keywordAdgroupidPtr)[keywordid]){
    //             if(checkHours(adgroup, hour)){
    //                 Adgroup nowAdgroup;
    //                 nowAdgroup.ctr = getCtr(userVector, (*keywordAdgroup2vectorPtr)[keywordid][index]);
    //                 if(ctr < 0){
    //                     continue;
    //                 }
    //                 nowAdgroup.price = (*keywordAdgroup2pricePtr)[keywordid][index];
    //                 nowAdgroup.score = nowAdgroup.ctr * nowAdgroup.price;
    //                 nowAdgroup.adgroup_id = adgroup;
    //                 if(nowAdgroup.score>65535){
    //                     continue;
    //                 }
    //                 adGroupBucket[(int)(nowAdgroup.score)].emplace_back(nowAdgroup);
    //             }
    //             index++;
    //         }
    //     }
        
    //     std::unordered_set<uint64_t> adgroupset;
    //     int index = 0, i = 0 , j = 0;
    //     for(i = 65535; i >= 0 && index < topn+1; i--){
    //         if(adGroupBucket[i].size() == 0){
    //             continue;
    //         }
    //         else if(adGroupBucket[i].size() > 1){
    //             std::sort(adGroupBucket[i].begin(), adGroupBucket[i].end());
    //         }
            
    //         for(j = 0 ; j < adGroupBucket[i].size() && index < topn+1 ; j++){
    //             if(adgroupset.find(adGroupBucket[i][j].adgroup_id) == adgroupset.end()){
    //                 if(index != topn){
    //                     response->add_adgroup_ids(adGroupBucket[i][j].adgroup_id);
    //                 }
    //                 if(index != 0){
    //                     response->add_prices(adGroupBucket[i][j].score/ctr+0.5);
    //                 }
    //                 ctr = adGroupBucket[i][j].ctr;
    //                 price = adGroupBucket[i][j].price;
    //                 adgroupset.insert(adGroupBucket[i][j].adgroup_id);
    //                 index++;
    //             }
    //         }
    //         if(index > topn){
    //             break;
    //         }
    //     }
    //     if(index < topn+1){
    //         response->add_prices(price);
    //     }
    // }

    // void getListQuicksort(const Request* request, Response* response){
    //     std::pair<float, float> userVector = std::make_pair(request->context_vector()[0], request->context_vector()[1]);
    //     uint32_t hour = request->hour();
    //     uint32_t topn = request->topn();
        
    //     int keywordLength =  request->keywords().size();
    //     float ctr;
    //     uint32_t price;
    //     float score;
    //     std::vector<Adgroup> adGroupPQ;
    //     std::unordered_map<uint64_t,uint32_t> adgroupmap;
    //     for(const auto & userKeyword : request->keywords()){
    //         auto keywordIt = keywordIDPtr->find(userKeyword);
    //         if (keywordIt == keywordIDPtr->end()) {
    //             continue;
    //         }
    //         int index = 0;
    //         const auto& keywordid = keywordIt->second;
    //         for(const auto& adgroup: (*keywordAdgroupidPtr)[keywordid]){
    //             if(checkHours(adgroup, hour)){
    //                 Adgroup nowAdgroup;
    //                 nowAdgroup.ctr = getCtr(userVector, (*keywordAdgroup2vectorPtr)[keywordid][index]);
    //                 nowAdgroup.price = (*keywordAdgroup2pricePtr)[keywordid][index];
    //                 nowAdgroup.score = nowAdgroup.ctr * nowAdgroup.price;
    //                 nowAdgroup.adgroup_id = adgroup;
    //                 if(adgroupmap.find(adgroup) == adgroupmap.end()){
    //                     adgroupmap[adgroup] = adGroupPQ.size();
    //                     adGroupPQ.emplace_back(nowAdgroup);
    //                 }
    //                 else{
    //                     size_t adgroupIndex = adgroupmap[adgroup];
    //                     if (nowAdgroup < adGroupPQ[adgroupIndex]) {
    //                         adGroupPQ[adgroupIndex].score = nowAdgroup.score;
    //                         adGroupPQ[adgroupIndex].ctr = nowAdgroup.ctr;
    //                         adGroupPQ[adgroupIndex].price = nowAdgroup.price;
    //                     }
    //                 }
    //             }
    //             index++;
    //         }
    //     }
    //     size_t k = std::min((size_t)topn+1, (size_t)adGroupPQ.size());
    //     std::nth_element(adGroupPQ.begin(), adGroupPQ.begin() + k - 1, adGroupPQ.end());
    //     std::sort(adGroupPQ.begin(), adGroupPQ.begin() + k);
    //     int index = 0;
    //     int i = 0;
    //     for(i = 0 ; i < adGroupPQ.size() && index < topn ; i++){
    //         response->add_adgroup_ids(adGroupPQ[i].adgroup_id);
    //         if(index != 0){
    //             response->add_prices(adGroupPQ[i].score/ctr+0.5);
    //         }
    //         ctr = adGroupPQ[i].ctr;
    //         price = adGroupPQ[i].price;
    //         index++;
            
    //     }
    //     if(index < topn){
    //         response->add_prices(price);
    //     }
    //     else{
    //         response->add_prices(adGroupPQ[i].score/ctr+0.5);
    //     }
    // }
};

}
#endif // SORT_H