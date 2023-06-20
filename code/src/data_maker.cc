#include <cmath>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <unordered_set>
#include <vector>

#include "id_gen.h"
#include "utils.h"

namespace alimama {

struct Campaign {
  id_t id;
  int32_t status : 8;
  int32_t timing_status : 24;
};

struct Adgroup {
  id_t id;
  size_t itemid;
  size_t campaign_idx;
  float vec[2];
};

struct Index {
  id_t keyword_id;
  size_t adgroup_idx;
  uint16_t price;
};

struct ValueRatio {
  int value;
  int ratio;  // by percent
};

// sum of value_ratio's ratio must be 100.
inline int GetValueByRatio(std::vector<ValueRatio> value_ratio) {
  int rand = Random(0, 100);
  int sum = 0;
  for (const auto& vr : value_ratio) {
    sum += vr.ratio;
    if (rand < sum) {
      return vr.value;
    }
  }
  return 0;
}

struct Option {
  size_t campaign_num = 5000000l;
  size_t adgroup_num = 7000000l;
  size_t keyword_num = 10000000l;
  std::vector<ValueRatio> adgroup_num_per_keyword{
      // clang-format off
      {500,     2},
      {200,     3},
      {100,     10},
      {50,      25},
      {25,      40},
      {5,       20},
      // clang-format on
  };

  std::vector<ValueRatio> campaign_status_ratio{
      // clang-format off
      {1, 95},
      {0, 5},
      // clang-format on
  };
  std::vector<ValueRatio> campaign_timing_status_ratio{
      // clang-format off
      {1, 20},
      {0, 80},
      // clang-format on
  };
};

void NormalizeVec(float* vec) {
  float x = Random(1, UINT32_MAX) * 1.0;
  float y = Random(1, UINT32_MAX) * 1.0;
  float root = std::sqrt(x * x + y * y);
  vec[0] = x / root;
  vec[1] = y / root;
}

void FormatOutput(const Index& index, const std::vector<Campaign>& campaigns,
                  const std::vector<Adgroup>& adgroups, std::ofstream* os) {
  const auto& adgroup = adgroups[index.adgroup_idx];
  const auto& campaign = campaigns[adgroup.campaign_idx];
  // format:
  //  keyword, adgroup, price, status, timing_status, vector, campaign, item
  (*os) << index.keyword_id << '\t';
  (*os) << adgroup.id << '\t';
  (*os) << index.price << '\t';
  (*os) << campaign.status << '\t';

  for (int i = 0; i < 24; ++i) {
    if (i > 0) (*os) << ',';
    (*os) << ((campaign.timing_status & (1 << i)) ? 1 : 0);
  }
  (*os) << '\t';

  (*os) << std::fixed << std::setprecision(6) << adgroup.vec[0] << ','
        << adgroup.vec[1] << '\t';
  (*os) << campaign.id << '\t';
  (*os) << adgroup.itemid << '\n';
  os->flush();
}

void MakeData(const Option& option, std::ofstream* os) {
  std::vector<Campaign> campaigns;
  campaigns.reserve(option.campaign_num);

  IdGen campaigns_id_gen;
  for (size_t i = 0; i < option.campaign_num; ++i) {
    Campaign campaign;
    campaign.id = campaigns_id_gen.Gen();
    campaign.status = GetValueByRatio(option.campaign_status_ratio);
    campaign.timing_status = 0;
    for (int i = 0; i < 24; ++i) {
      int s = GetValueByRatio(option.campaign_timing_status_ratio);
      campaign.timing_status |= (s << i);
    }
    campaigns.push_back(campaign);
  }

  std::vector<Adgroup> adgroups;
  adgroups.reserve(option.adgroup_num);

  IdGen adgroup_id_gen;
  IdGen item_id_gen;
  for (size_t i = 0; i < option.adgroup_num; ++i) {
    Adgroup adgroup;
    adgroup.id = adgroup_id_gen.Gen();
    adgroup.itemid = item_id_gen.Gen();
    adgroup.campaign_idx = i % option.campaign_num;
    NormalizeVec(adgroup.vec);
    adgroups.push_back(adgroup);
  }

  IdGen keyword_id_gen;
  for (size_t i = 0; i < option.keyword_num; ++i) {
    Index index;
    index.keyword_id = keyword_id_gen.Gen();

    std::unordered_set<size_t> adgroup_idx_set;
    int adgroup_num = GetValueByRatio(option.adgroup_num_per_keyword);
    adgroup_num += Random(0, adgroup_num / 2) - adgroup_num / 4;
    for (int j = 0; j < adgroup_num; ++j) {
      size_t adgroup_idx;
      do {
        adgroup_idx = Random(0, adgroups.size() - 1);
      } while (adgroup_idx_set.find(adgroup_idx) != adgroup_idx_set.end());
      adgroup_idx_set.insert(adgroup_idx);
      index.adgroup_idx = adgroup_idx;
      index.price = Random(1, 65535);
      FormatOutput(index, campaigns, adgroups, os);
    }
  }
}

}  // namespace alimama

bool ParseValueRatio(const std::string& str,
                     std::vector<alimama::ValueRatio>* vrs) {
  vrs->clear();
  std::vector<std::string_view> vec;
  alimama::SplitStr(str, ",", &vec);
  if (vec.size() == 0 || vec.size() % 2 != 0) {
    ALI_LOG("ratio format error: must in <value>,<ratio>,<value>,<ratio>. ", str);
    return false;
  }
  for (size_t i = 0; i < vec.size() - 1; i += 2) {
    alimama::ValueRatio vr{atoi(vec[i].data()), atoi(vec[i + 1].data())};
    vrs->push_back(vr);
  }
  int sum = 0;
  for (auto& v : *vrs) {
    sum += v.ratio;
  }
  if (sum != 100) {
    ALI_LOG("ratio format error: sum of ratio must be 100. ", str);
    return false;
  }
  return true;
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    std::cout << "Usage: \n";
    std::cout << "\tdata_maker [options] output_file\n\n";
    std::cout << "Options: \n";
    std::cout << "\t--key_num keyword number\n";
    std::cout << "\t--camp_num campaign number\n";
    std::cout << "\t--ad_num adgroup number\n";
    std::cout << "\t--adnum_per_key adgroup number per keyword\n";
    std::cout << "\t--camp_s_r campaign status ratio\n";
    std::cout << "\t--camp_ts_r campaign timing status ratio\n";
    return 0;
  }
  alimama::Option option;
  for (int i = 1; i < argc - 2; i += 2) {
    if (STR_EQ(argv[i], "--key_num")) {
      option.keyword_num = atol(argv[i + 1]);
    } else if (STR_EQ(argv[i], "--camp_num")) {
      option.campaign_num = atol(argv[i + 1]);
    } else if (STR_EQ(argv[i], "--ad_num")) {
      option.adgroup_num = atol(argv[i + 1]);
    } else if (STR_EQ(argv[i], "--adnum_per_key")) {
      if (!ParseValueRatio(argv[i + 1], &option.adgroup_num_per_keyword)) {
        ALI_LOG("--adnum_per_key options set fail. ", argv[i + 1]);
        return -1;
      }
    } else if (STR_EQ(argv[i], "--camp_s_r")) {
      if (!ParseValueRatio(argv[i + 1], &option.campaign_status_ratio)) {
        ALI_LOG("--camp_s_r options set fail. ", argv[i + 1]);
        return -1;
      }
    } else if (STR_EQ(argv[i], "--camp_ts_r")) {
      if (!ParseValueRatio(argv[i + 1], &option.campaign_timing_status_ratio)) {
        ALI_LOG("--camp_ts_r options set fail. ", argv[i + 1]);
        return -1;
      }
    } else {
      ALI_LOG("unkown options: ", argv[i]);
      return -1;
    }
  }

  std::ofstream ofile{argv[argc - 1], std::ios::trunc};
  if (!ofile.good()) {
    ALI_LOG("open file for writing fail. file: ", argv[argc - 1]);
    return -1;
  }
  ofile << "keyword\tadgroup\tprice\tstatus\ttiming_status\tvector\tcampaign\titem" << std::endl;
  alimama::MakeData(option, &ofile);
  return 0;
}