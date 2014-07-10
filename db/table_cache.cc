// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/table_cache.h"

#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/table.h"
#include "util/mutexlock.h"
#include "util/coding.h"

namespace leveldb {

struct TableAndFile {
  RandomAccessFile* file;
  Table* table;
  TableCache* table_cache;
  int use_count;
  port::Mutex mutex;
};

static void DeleteEntry(const Slice& key, void* value) {
  TableAndFile* tf = reinterpret_cast<TableAndFile*>(value);
  tf->table_cache->ForceClose();
  delete tf->table;
  delete tf->file;
  delete tf;
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

TableCache::TableCache(const std::string& dbname,
                       const Options* options,
                       int entries)
    : env_(options->env),
      dbname_(dbname),
      options_(options),
      cache_(NewLRUCache(entries)),
      file_to_close_(NULL) {
}

TableCache::~TableCache() {
  delete cache_;
}

Status TableCache::FindTable(uint64_t file_number, uint64_t file_size,
                             Cache::Handle** handle) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  *handle = cache_->Lookup(key);
  if (*handle == NULL) {
    std::string fname = TableFileName(dbname_, file_number);
    RandomAccessFile* file = NULL;
    Table* table = NULL;
    s = env_->NewRandomAccessFile(fname,
                                  options_->low_open_files_mode_enabled,
                                  &file);
    if (!s.ok()) {
      std::string old_fname = SSTTableFileName(dbname_, file_number);
      if (env_->NewRandomAccessFile(old_fname,
                                    options_->low_open_files_mode_enabled,
                                    &file).ok()) {
        s = Status::OK();
      }
    }
    if (s.ok()) {
      s = file->Open();
    }
    if (s.ok()) {
      s = Table::Open(*options_, this, file, file_number, file_size, &table);
    }

    if (!s.ok()) {
      assert(table == NULL);
      delete file;
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      TableAndFile* tf = new TableAndFile;
      tf->file = file;
      tf->table = table;
      tf->use_count = 1;
      tf->table_cache = this;
      *handle = cache_->Insert(key, tf, 1, &DeleteEntry);
      if (options_->low_open_files_mode_enabled) {
        CloseFile(file_number);
      }
    }
  }
  return s;
}

Iterator* TableCache::NewIterator(const ReadOptions& options,
                                  uint64_t file_number,
                                  uint64_t file_size,
                                  Table** tableptr) {
  if (tableptr != NULL) {
    *tableptr = NULL;
  }

  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (!s.ok()) {
    return NewErrorIterator(s);
  }

  Table* table = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
  Iterator* result = table->NewIterator(options);
  result->RegisterCleanup(&UnrefEntry, cache_, handle);
  if (tableptr != NULL) {
    *tableptr = table;
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       uint64_t file_number,
                       uint64_t file_size,
                       const Slice& k,
                       void* arg,
                       void (*saver)(void*, const Slice&, const Slice&)) {
  Cache::Handle* handle = NULL;
  Status s = FindTable(file_number, file_size, &handle);
  if (s.ok()) {
    Table* t = reinterpret_cast<TableAndFile*>(cache_->Value(handle))->table;
    s = t->InternalGet(options, k, arg, saver);
    cache_->Release(handle);
  }
  return s;
}

void TableCache::Evict(uint64_t file_number) {
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  cache_->Erase(Slice(buf, sizeof(buf)));
}

Status TableCache::OpenFile(uint64_t file_number) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  Cache::Handle* handle = NULL;
  handle = cache_->Lookup(key);
  if (handle != NULL) {
    TableAndFile* tf = reinterpret_cast<TableAndFile*>(cache_->Value(handle));
    MutexLock l(&tf->mutex);
    ++tf->use_count;
    if (tf->use_count == 1) {
      MutexLock l(&mutex_);
      if (file_to_close_ == tf->file) {
        // Use same file.
        file_to_close_ = NULL;
      } else {
        if (file_to_close_ != NULL) {
          file_to_close_->Close();
          file_to_close_ = NULL;
        }
        s = tf->file->Open();
        if (!s.ok()) {
          --tf->use_count;
        }
      }
    }
  }
  return s;
}

void TableCache::CloseFile(uint64_t file_number) {
  Status s;
  char buf[sizeof(file_number)];
  EncodeFixed64(buf, file_number);
  Slice key(buf, sizeof(buf));
  Cache::Handle* handle = NULL;
  handle = cache_->Lookup(key);
  if (handle != NULL) {
    TableAndFile* tf = reinterpret_cast<TableAndFile*>(cache_->Value(handle));
    MutexLock l(&tf->mutex);
    --tf->use_count;
    if (tf->use_count == 0) {
      MutexLock l(&mutex_);
      if (file_to_close_ != NULL) {
        file_to_close_->Close();
        file_to_close_ = NULL;
      }
      file_to_close_ = tf->file;
    }
  }
}

void TableCache::ForceClose()
{
  MutexLock l(&mutex_);
  if (file_to_close_ != NULL) {
    file_to_close_->Close();
    file_to_close_ = NULL;
  }
}

}  // namespace leveldb
