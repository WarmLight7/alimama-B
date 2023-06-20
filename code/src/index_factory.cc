#include "index_factory.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>
#include <iostream>
#include <mutex>
#include <thread>

#include "utils.h"

namespace alimama {
IndexFactory::IndexFactory() = default;

bool IndexFactory::LoadIndexText(const IndexFactory::Option& option,
                                 const std::string& data_file) {
  ALI_LOG("Load Start.");
  TimeKeeper tk;

  Index index;
  index.resize(option.segment_num);

  auto load_func = [&index, &option, &data_file](size_t i) {
    ALI_LOG("Thread ", i, " start.");
    TimeKeeper tk;
    auto& segment = index[i];

    std::string line;
    std::ifstream ifs_alldata(data_file);
    std::vector<std::string_view> sections;
    while (std::getline(ifs_alldata, line)) {
      SplitStr(line, "\t", &sections);
      if (sections.size() != 8) continue;

      char* end_dummy = nullptr;
      id_t key_id = strtoull(sections[0].data(), &end_dummy, 10);
      if (key_id % option.segment_num != i) continue;

      // filter status offline
      if (sections[3].data()[0] != '1') {
        continue;
      }

      // filter by column
      id_t adgroup_id = strtoull(sections[1].data(), &end_dummy, 10);
      if (adgroup_id % option.column_num != option.column_id) {
        continue;
      }
      Payload payload;
      payload.adgroup_id = adgroup_id;

      payload.price = strtoull(sections[2].data(), &end_dummy, 10);

      payload.timing_status = 0;
      for (int i = 0; i < 24; ++i) {
        int32_t v = sections[4].data()[i * 2] == '0' ? 0 : 1;
        payload.timing_status |= v << i;
      }

      sscanf(sections[5].data(), "%f,%f", &payload.vec[0], &payload.vec[1]);
      segment[key_id].push_back(payload);
    }
    ALI_LOG("Thread ", i, " cost: ", tk.Escape(), "ms");
  };

  if (option.segment_num == 1) {
    load_func(0);
  } else {
    std::vector<std::thread> threads;
    for (size_t i = 0; i < option.segment_num; ++i) {
      threads.emplace_back(load_func, i);
    }
    for (auto& th : threads) {
      th.join();
    }
  }

  inited_ = true;
  option_ = option;
  index_ = std::move(index);
  ALI_LOG("Load End. cost: ", tk.Escape(), "ms");
  return true;
}

}  // namespace alimama