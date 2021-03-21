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

  std::lock_guard<std::mutex> guard(control); // Lock applied as changes are made to internal data structures
  
  if(cacheIndex.size()!=0) {
   
    // The cache is not empty
  	*frame_id = cacheList.back();
  	cacheList.pop_back();
  	cacheIndex.erase(*frame_id);
  	return true;
  }
  return false;
}

void LRUReplacer::Pin(frame_id_t frame_id) {

  std::lock_guard<std::mutex> guard(control); // Lock applied as changes are made to internal data structures
  
  if(cacheIndex.find(frame_id)!=cacheIndex.end()) {
  
  	// Frame is present in the cache 
  	cacheList.erase(cacheIndex[frame_id]);
  	cacheIndex.erase(frame_id); 
  } 
}

void LRUReplacer::Unpin(frame_id_t frame_id) {

  std::lock_guard<std::mutex> guard(control); // Lock applied as changes are made to internal data structures
  
  if(cacheIndex.find(frame_id)==cacheIndex.end()){
  
  	// Frame is not present in the cache
  	cacheList.push_front(frame_id);
    cacheIndex[frame_id] = cacheList.begin();
  }
}

size_t LRUReplacer::Size() { return cacheIndex.size(); }

}  // namespace bustub
