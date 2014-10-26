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
 
#include "exec/nested-loop-join-node.h"
 
#include <sstream>
 
#include "codegen/llvm-codegen.h"
#include "exprs/expr.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"
 
#include "gen-cpp/PlanNodes_types.h"
 
using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;
 
/**
 * Constructor
 */
NestedLoopJoinNode::NestedLoopJoinNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : BlockingJoinNode("NestedLoopJoinNode", TJoinOp::INNER_JOIN, pool, tnode, descs) {
}
 
/**
 * Call parent's Prepare() before doing any other processing.
 * Initialize any row pools or batches for the right child here 
 */
Status NestedLoopJoinNode::Prepare(RuntimeState* state) {
  RETURN_IF_ERROR(BlockingJoinNode::Prepare(state));

  return Status::OK;
}
 
/**
 * Close any open right_child structures, such as row batches or pool
 */
void NestedLoopJoinNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  BlockingJoinNode::Close(state);
}
 
/**
 * Do a full scan of the right child [child(1)] and store all row batches
 * in right_child_batches_
 */
Status NestedLoopJoinNode::ConstructBuildSide(RuntimeState* state) {
  RETURN_IF_ERROR(child(1)->Open(state));
  while (true) {
    RowBatch* batch = right_child_pool_->Add(
        new RowBatch(child(1)->row_desc(), state->batch_size(), mem_tracker()));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->CheckQueryState());
    bool eos;
    RETURN_IF_ERROR(child(1)->GetNext(state, batch, &eos));
    DCHECK_EQ(batch->num_io_buffers(), 0) << "Build batch should be compact.";
    SCOPED_TIMER(build_timer_);
    right_child_batches_.AddRowBatch(batch);
    VLOG_ROW << BuildListDebugString();
    COUNTER_SET(build_row_counter_,
        static_cast<int64_t>(right_child_batches_.total_num_rows()));
    if (eos) break;
  }
  return Status::OK;
}
 
void NestedLoopJoinNode::InitGetNext(TupleRow* first_left_row) {
  current_right_child_row_ = right_child_batches_.Iterator();
}
 
/**
 * Process each row from the left child, until we eos_ is true (i.e. end of stream)
 */
Status NestedLoopJoinNode::GetNext(RuntimeState* state, RowBatch* output_batch, bool* eos) {
  if (ReachedLimit() || eos_) {
    *eos = true;
    return Status::OK;
  }
 
  //ScopedTimer<MonotonicStopWatch> timer(left_child_timer_);
  while (!eos_) {
    // Compute max rows that should be added to output_batch
    int64_t row_batch_capacity = GetRowBatchCapacity(output_batch);
 
    // Continue processing this row batch
    num_rows_returned_ +=
        DoNestedLoopJoin(output_batch, left_batch_.get(), row_batch_capacity);
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);
 
    if (ReachedLimit() || output_batch->AtCapacity()) {
      *eos = ReachedLimit();
      break;
    }
 
    // Check to see if we're done processing the current left child batch
    if (current_right_child_row_.AtEnd() && left_batch_pos_ == left_batch_->num_rows()) {
      left_batch_->TransferResourceOwnership(output_batch);
      left_batch_pos_ = 0;
      if (output_batch->AtCapacity()) break;
      if (left_side_eos_) {
        *eos = eos_ = true;
        break;
      } else {
        child(0)->GetNext(state, left_batch_.get(), &left_side_eos_);
        COUNTER_UPDATE(left_child_row_counter_, left_batch_->num_rows());
      }
    }
  }
 
  return Status::OK;
}
 
int64_t NestedLoopJoinNode::GetRowBatchCapacity(RowBatch* output_batch){
  int64_t max_added_rows = output_batch->capacity() - output_batch->num_rows();
  if (limit() != -1)
    max_added_rows = min(max_added_rows, limit() - rows_returned());
  return max_added_rows;
}
 
string NestedLoopJoinNode::BuildListDebugString() {
  stringstream out;
  out << "BuildList(";
  out << right_child_batches_.DebugString(child(1)->row_desc());
  out << ")";
  return out.str();
}
 
int NestedLoopJoinNode::DoNestedLoopJoin(RowBatch* output_batch, RowBatch* batch,
    int row_batch_capacity) {
  return 0;
}