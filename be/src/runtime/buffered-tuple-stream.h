// Copyright 2013 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef IMPALA_RUNTIME_BUFFERED_TUPLE_STREAM_H
#define IMPALA_RUNTIME_BUFFERED_TUPLE_STREAM_H

#include "common/status.h"
#include "runtime/buffered-block-mgr.h"

namespace impala {

class BufferedBlockMgr;
class RuntimeProfile;
class RuntimeState;
class RowBatch;
class RowDescriptor;
class SlotDescriptor;
class TupleRow;

// Class that provides an abstraction for a stream of tuple rows. Rows can be
// added to the stream and returned. Rows are returned in the order they are added.
//
// The underlying memory management is done by the BufferedBlockMgr.
//
// The BufferedTupleStream is *not* thread safe from the caller's point of view. It is
// expected that all the APIs are called from a single thread. Internally, the
// object is thread safe wrt to the underlying block mgr.
//
// Buffer management:
// The stream is either pinned or unpinned, set via PinAllBlocks() and UnpinAllBlocks().
// PinAllBlocks() will pin all unpinned blocks, if possible. UnpinAllBlocks() will unpin
// all blocks except the write_block_ and the read_block_ (if read_write is true). Blocks
// are optionally deleted as they are read, set with the delete_on_read c'tor parameter.
//
// The behavior of reads and writes is as follows:
// Read:
//   1. Delete on read (delete_on_read_): Blocks are deleted as we go through the stream.
//   The data returned by the tuple stream is valid until the next read call so the
//   caller does not need to copy if it is streaming.
//   2. Unpinned: Blocks remain in blocks_ and are unpinned after reading.
//   3. Pinned: Blocks remain in blocks_ and are left pinned after reading. If the next
//   block in the stream cannot be pinned, the read call will fail and the caller needs
//   to free memory from the underlying block mgr.
// Write:
//   1. Unpinned: Unpin blocks as they fill up. This means only a single (i.e. the
//   current) block needs to be in memory regardless of the input size (if read_write is
//   true, then two blocks need to be in memory).
//   2. Pinned: Blocks are left pinned. If we run out of blocks, the write will fail and
//   the caller needs to free memory from the underlying block mgr.
//
// Tuple row layout: Tuples are stored back to back. Each tuple starts with the fixed
// length portion, directly followed by the var len portion. (Fixed len and var len
// are interleaved).
//
// TODO: we need to be able to do read ahead in the BufferedBlockMgr. It currently
// only has PinAllBlocks() which is blocking. We need a non-blocking version of this or
// some way to indicate a block will need to be pinned soon.
// TODO: see if this can be merged with Sorter::Run. The key difference is that this
// does not need to return rows in the order they were added, which allows it to be
// simpler.
// TODO: improvements:
//   - Think about how to layout for the var len data more, possibly filling in them
//     from the end of the same block. Don't interleave fixed and var len data.
//   - We will want to multithread this. Add a AddBlock() call so the synchronization
//     happens at the block level. This is a natural extension.
//   - Instead of allocating all blocks from the block_mgr, allocate some blocks that
//     are much smaller (e.g. 16K and doubling up to the block size). This way, very
//     small streams (a common case) will use very little memory. This small blocks
//     are always in memory since spilling them frees up negligible memory.
//   - Return row batches in GetNext() instead of filling one in.
class BufferedTupleStream {
 public:
  // row_desc: description of rows stored in the stream. This is the desc for rows
  // that are added and the rows being returned.
  // block_mgr: Underlying block mgr that owns the data blocks.
  // delete_on_read: Blocks are deleted after they are read.
  // read_write: Stream allows interchanging read and write operations. Requires at
  // least two blocks may be pinned.
  // The tuple stream is initially in pinned mode.
  BufferedTupleStream(RuntimeState* state, const RowDescriptor& row_desc,
      BufferedBlockMgr* block_mgr, BufferedBlockMgr::Client* client,
      bool delete_on_read = true, bool read_write = false);

  // Initializes the tuple stream object. Must be called once before any of the
  // other APIs.
  Status Init();

  // Adds a single row to the stream. Returns false if an error occurred.
  // BufferedTupleStream will do a deep copy of the memory in the row.
  bool AddRow(TupleRow* row);

  // Allocates space to store a row of size 'size'. Returns NULL if there is
  // not enough memory. The returned memory is guaranteed to fit on one block.
  uint8_t* AllocateRow(int size);

  // Prepares the stream for reading. If read_write_, this does not need to be called in
  // order to begin reading, otherwise this must be called after the last AddRow() and
  // before GetNext().
  Status PrepareForRead();

  // Pins all blocks in this stream and switches to pinned mode.
  Status PinAllBlocks(bool* pinned);

  // Unpins all blocks except the write_block_ and the read_block_ if read_write_ is
  // true. Switches to unpinned mode.
  Status UnpinAllBlocks();

  // Get the next batch of output rows. Memory is still owned by the BufferedTupleStream
  // and must be copied out by the caller.
  Status GetNext(RowBatch* batch, bool* eos);

  // Must be called once at the end to cleanup all resources.
  void Close();

  // Returns the status of the stream. We don't want to return a more costly Status
  // object on AddRow() which is way that API returns a bool.
  Status status() const { return status_; }

  // Number of rows in the stream.
  int64_t num_rows() const { return num_rows_; }

  // Number of rows returned via GetNext().
  int64_t rows_returned() const { return rows_returned_; }

  // Returns the byte size necessary to store the entire stream in memory.
  int64_t byte_size() const { return blocks_.size() * block_mgr_->block_size(); }

  // Returns the byte size of the stream that is currently pinned in memory.
  // If ignore_current is true, the write_block_ memory is not included.
  int64_t bytes_in_mem(bool ignore_current) const;

  // Returns the number of bytes that are in unpinned blocks.
  int64_t bytes_unpinned() const;

 private:
  // If true, blocks are deleted after they are read.
  const bool delete_on_read_;

  // If true, read and write operations may be interleaved. Otherwise all calls
  // to AddRow() must occur before calling PrepareForRead() and subsequent calls to
  // GetNext().
  const bool read_write_;

  // Runtime state instance used to check for cancellation. Not owned.
  RuntimeState* const state_;

  // Description of rows stored in the stream.
  const RowDescriptor& desc_;

  // Sum of the fixed length portition of all the tuples in desc_.
  int fixed_tuple_row_size_;

  // Vector of all the strings slots grouped by tuple_idx.
  std::vector<std::pair<int, std::vector<SlotDescriptor*> > > string_slots_;

  // Block manager and client used to allocate, pin and release blocks. Not owned.
  BufferedBlockMgr* block_mgr_;
  BufferedBlockMgr::Client* block_mgr_client_;

  // List of blocks in the stream.
  std::list<BufferedBlockMgr::Block*> blocks_;

  // Iterator pointing to the current block for read. If read_write_, this is always a
  // valid block, otherwise equal to list.end() until PrepareForRead() is called.
  std::list<BufferedBlockMgr::Block*>::iterator read_block_;

  // Current ptr offset in read_block_'s buffer.
  uint8_t* read_ptr_;

  // Bytes read in read_block_.
  int64_t read_bytes_;

  // Number of rows returned to the caller from GetNext().
  int64_t rows_returned_;

  // The current block for writing. NULL if there is no available block to write to.
  BufferedBlockMgr::Block* write_block_;

  // Number of pinned blocks in blocks_, stored to avoid iterating over the list
  // to compute bytes_in_mem and bytes_unpinned.
  int num_pinned_;

  Status status_;

  // Number of rows stored in the stream.
  int64_t num_rows_;

  // If true, this stream has been explicitly pinned by the caller. This changes the
  // memory management of the stream. The blocks are not unpinned until the caller calls
  // UnpinAllBlocks(). If false, only the write_block_ and/or read_block_ are pinned
  // (both are if read_write_ is true).
  bool pinned_;

  // Copies row into 'write_block_'. Returns false if there is not enough space
  // in 'write_block_'.
  bool DeepCopy(TupleRow* row);

  // Gets a new block from the block_mgr_, updating write_block_ and
  // setting *got_block. If there are no blocks available, write_block_ is set to NULL
  // and *got_block is set to false.
  Status NewBlockForWrite(bool* got_block);

  // Reads the next block from the block_mgr_. This blocks if necessary.
  // Updates read_block_, read_ptr_ and read_bytes_left_.
  Status NextBlockForRead();

  std::string DebugString() const;
};

}

#endif
