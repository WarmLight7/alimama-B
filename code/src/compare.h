#pragma once

#include "search.pb.h"

namespace alimama {

inline bool AdComp(const proto::Ad& l, const proto::Ad& r) {
  if (l.score() > r.score()) return true;
  if (l.score() < r.score()) return false;
  if (l.ctr() > r.ctr()) return true;
  return false;
};

}  // namespace alimama