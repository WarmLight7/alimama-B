#pragma once

#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

#include "types.h"
#include "utils.h"

namespace alimama {

struct Payload {
  id_t adgroup_id;
  uint32_t timing_status;
  float vec[2];
  uint16_t price;
};

typedef std::vector<Payload> PayloadList;
typedef std::unordered_map<id_t, PayloadList> SegmentIndex;
typedef std::vector<SegmentIndex> Index;

// TODO:
struct BinIndex {};

class IndexFactory {
 public:
  IndexFactory();

  struct Option {
    column_id_t column_id = 0;
    column_id_t column_num = 1;
    segment_id_t segment_num = 16;
  };

  static bool BuildIndex(const Option& option, const std::string& raw_file,
                         const std::string& index_path);

  bool LoadIndexBinary(const Option& option, const std::string& index_path);
  bool LoadIndexText(const Option& option, const std::string& data_file);

  bool SaveToBinary(const std::string& index_path);

  template <typename C>
  bool Search(const C& keywords, std::vector<PayloadList*>* docs) {
    if (!inited_) {
      ALI_LOG("not init.");
      return false;
    }

    docs->reserve(keywords.size());

    if (index_.index() == 0) {
      auto& index = std::get<0>(index_);
      for (auto& key : keywords) {
        auto& segment = index[key % option_.segment_num];
        auto it = segment.find(key);
        if (it == segment.end()) {
          docs->push_back(nullptr);
        } else {
          docs->push_back(&it->second);
        }
      }

    } else {
    }

    return true;
  }

 private:
  bool inited_ = false;
  Option option_;
  std::variant<Index, BinIndex> index_;
};

}  // namespace alimama