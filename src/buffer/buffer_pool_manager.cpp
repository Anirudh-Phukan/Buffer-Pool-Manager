//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <list>
#include <unordered_map>

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete replacer_;
}

Page *BufferPoolManager::FetchPageImpl(page_id_t page_id) {
  std::lock_guard<std::mutex> guard(latch_);

  frame_id_t empty_frame;
  
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  if (page_table_.find(page_id) != page_table_.end()) {
    empty_frame = page_table_[page_id];
    replacer_->Pin(empty_frame);
    pages_[empty_frame].pin_count_ += 1;
    return &pages_[empty_frame];
  }

  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  if (!free_list_.empty()) {
    empty_frame = free_list_.front();
    free_list_.pop_front();
  } 
  
  else {
    if (replacer_->Victim(&empty_frame)) {
		
		// 2.     If R is dirty, write it back to the disk.
		Page *p = &pages_[empty_frame];
		
		if (p->is_dirty_) {
		  FlushPageImpl(p->page_id_);
		  p->is_dirty_ = false;
		}
		
		// 3.     Delete R from the page table and insert P.
		page_table_.erase(p->page_id_);
    }
    
    else{
    	return nullptr;
    }
  }

  // 4   read in the page content from disk
  pages_[empty_frame].ResetMemory();
  disk_manager_->ReadPage(page_id, pages_[empty_frame].GetData());

  // 5  Update P's metadata, and then return a pointer to P.
  pages_[empty_frame].page_id_ = page_id;
  page_table_.insert({page_id, empty_frame});
  pages_[empty_frame].pin_count_ = 1;
  pages_[empty_frame].is_dirty_ = false;  
  replacer_->Pin(empty_frame);

  return &pages_[empty_frame];
}

bool BufferPoolManager::UnpinPageImpl(page_id_t page_id, bool is_dirty) {

  std::lock_guard<std::mutex> guard(latch_); // Lock applied as buffer pool is modified	 
  
  if (page_table_.find(page_id) != page_table_.end()) {
  
    // The page is found in the buffer pool
    Page *p = &pages_[page_table_[page_id]];
    
    if(p->pin_count_ > 0){
    
    	(p->pin_count_)--;
    	p->is_dirty_ = is_dirty;
    	
    	if (p->pin_count_ == 0) {
			replacer_->Unpin(page_table_[page_id]);
	    }
	    
	    return true;
    }
    
    return false;
  }
  
  // The page is not present in the buffer pool manager
  return false;
}

bool BufferPoolManager::FlushPageImpl(page_id_t page_id) {

  if (page_table_.find(page_id) != page_table_.end()) {
  	  
  	  // The page has been found in the buffer pool manager	
	  Page *p = &pages_[page_table_[page_id]];

	  if (p->is_dirty_) { 
		disk_manager_->WritePage(page_id, p->GetData()); 
		p->is_dirty_ = false; 
	  }
	  
	  return true;
  }
  
    // The page is not present in the buffer pool manager	
	return false;
}

Page *BufferPoolManager::NewPageImpl(page_id_t *page_id) {
  // 0.   Make sure you call DiskManager::AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  
  std::lock_guard<std::mutex> guard(latch_);	
  
  frame_id_t empty_frame;
  
  if (!free_list_.empty()) {
    empty_frame = free_list_.front();
    free_list_.pop_front();
  } else {
    if (replacer_->Victim(&empty_frame)) {
    
		Page *p = &pages_[empty_frame];
		
		if (p->is_dirty_) {
		  FlushPageImpl(p->page_id_);
		  p->is_dirty_ = false;
		}
		
		page_table_.erase(p->page_id_);
    }
    else{
    	return nullptr;
    }
  }

  *page_id = disk_manager_->AllocatePage();
  
  pages_[empty_frame].page_id_ = *page_id; // Updating P metadata
  pages_[empty_frame].ResetMemory(); // Zero out memory
  page_table_.insert({*page_id, empty_frame}); // Adding P to the page table
  
  replacer_->Pin(empty_frame);
  pages_[empty_frame].pin_count_ = 1;
  pages_[empty_frame].is_dirty_ = false;  

  return &pages_[empty_frame];
}

bool BufferPoolManager::DeletePageImpl(page_id_t page_id) {

  std::lock_guard<std::mutex> guard(latch_); // Lock applied as buffer pool is modified
  
  disk_manager_->DeallocatePage(page_id); // 0.   Make sure you call DiskManager::DeallocatePage!
  
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  if (page_table_.find(page_id) == page_table_.end()) {
    return true;
  }
  
  Page *p = &pages_[page_table_[page_id]];
  
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.	
  if (p->pin_count_ > 0) {
    return false;
  }
  
   // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  page_table_.erase(p->page_id_); // Removing P from the page table
  
  // Reseting metadata
  p->ResetMemory();
  p->page_id_ = INVALID_PAGE_ID;
  p->is_dirty_ = false;
  p->pin_count_ = 0;
  
  // Returning back to the free list
  free_list_.push_back(page_table_[page_id]);
  return true;
}

void BufferPoolManager::FlushAllPagesImpl() {
  size_t ctr = 0;
  while(ctr < pool_size_){
  	FlushPageImpl(pages_[ctr].page_id_); // Flushing every page currently present in the buffer pool manager
  	ctr++;
  }
}

}  // namespace bustub
