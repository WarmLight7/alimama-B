#include "index_factory.h"

int main(int argc, char* argv[]) {
  alimama::IndexFactory index_factory;

  alimama::IndexFactory::Option option;
  option.column_id = 0;
  option.column_num = 1;
  option.segment_num = 16;

  index_factory.LoadIndexText(option, argv[1]);

  std::vector<alimama::PayloadList*> docs;

  std::vector<id_t> keys;
  keys.push_back(2916200016);
  index_factory.Search(keys, &docs);

  return 0;
}