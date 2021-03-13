//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) {
  cacheIndex.reserve(this->num_pages);
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::lock_guard<std::mutex> guard(control);
  if(Size()!=0) { 
  	*frame_id = cacheList.back();
  	cacheList.pop_back();
  	cacheIndex.erase(*frame_id);
  	return true;
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(control);
  if(cacheIndex.find(frame_id)!=cacheIndex.end()) { 
  	cacheList.erase(cacheIndex[frame_id]);
  	cacheIndex.erase(frame_id); 
  } 
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::lock_guard<std::mutex> guard(control);
  if(cacheIndex.find(frame_id)==cacheIndex.end()){
  	cacheList.push_front(frame_id);
    cacheIndex[frame_id] = cacheList.begin();
  }
}

size_t LRUReplacer::Size() { return cacheIndex.size(); }

}  // namespace bustub
