#include "exec/csv-scan-node.h"

#include <boost/scoped_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <sstream>

#include "runtime/runtime-state.h"
#include "runtime/row-batch.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/runtime-profile.h"
#include "gen-cpp/PlanNodes_types.h"

#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string.hpp>
#include "runtime/exec-env.h"
#include "statestore/statestore-subscriber.h"
#include <boost/mpl/vector.hpp>

using namespace impala;
using namespace std;
using namespace boost;

CsvScanNode::CsvScanNode(ObjectPool* pool, const TPlanNode& tnode,
		const DescriptorTbl& descs) :
		ExecNode(pool, tnode, descs), runtime_state_(NULL), tuple_desc_(NULL), tuple_id_(
				tnode.csv_scan_node.tuple_id), path_(tnode.path), tuple_(NULL), batch_(
				NULL), done_(false) {
	max_materialized_row_batches_ = 10 * DiskInfo::num_disks();
	materialized_row_batches_.reset(
			new RowBatchQueue(max_materialized_row_batches_));
}

CsvScanNode::~CsvScanNode() {
}

Status CsvScanNode::Prepare(RuntimeState* state) {
	runtime_state_ = state;
	RETURN_IF_ERROR(ExecNode::Prepare(state));

	tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
	DCHECK(tuple_desc_ != NULL);

	num_null_bytes_ = tuple_desc_->num_null_bytes();

	// Create mapping from column index in table to slot index in output tuple.
	// First, initialize all columns to SKIP_COLUMN.
	int num_cols = tuple_desc_->table_desc()->num_cols();
	column_idx_to_materialized_slot_idx_.resize(num_cols);
	for (int i = 0; i < num_cols; ++i) {
		column_idx_to_materialized_slot_idx_[i] = SKIP_COLUMN;
	}

	// Next, collect all materialized slots
	vector<SlotDescriptor*> all_materialized_slots;
	all_materialized_slots.resize(num_cols);
	const vector<SlotDescriptor*>& slots = tuple_desc_->slots();
	for (size_t i = 0; i < slots.size(); ++i) {
		if (!slots[i]->is_materialized())
			continue;
		int col_idx = slots[i]->col_pos();
		DCHECK_LT(col_idx, column_idx_to_materialized_slot_idx_.size());
		DCHECK_EQ(column_idx_to_materialized_slot_idx_[col_idx], SKIP_COLUMN);
		all_materialized_slots[col_idx] = slots[i];
	}

	// Finally, populate materialized_slots_ in the order that
	// the slots appear in the file.
	for (int i = 0; i < num_cols; ++i) {
		SlotDescriptor* slot_desc = all_materialized_slots[i];
		if (slot_desc == NULL)
			continue;
		else {
			column_idx_to_materialized_slot_idx_[i] =
					materialized_slots_.size();
			materialized_slots_.push_back(slot_desc);
		}
	}

	return Status::OK;
}

void CsvScanNode::StartNewRowBatch() {
	batch_ = new RowBatch(row_desc(), runtime_state_->batch_size(),
			mem_tracker());
	tuple_mem_ = batch_->tuple_data_pool()->Allocate(
			runtime_state_->batch_size() * tuple_desc_->byte_size());
}

Status CsvScanNode::Open(RuntimeState* state) {
        std::string file_name = GetFilePath();
        csv_file_.open(file_name.c_str());
        num_rows_ = 0;
        StartNewRowBatch();
	return Status::OK;
}

// Do not change this method
int CsvScanNode::GetMemory(MemPool** pool, Tuple** tuple_mem,
		TupleRow** tuple_row_mem) {
	DCHECK(batch_ != NULL);
	DCHECK(!batch_->AtCapacity());
	*pool = batch_->tuple_data_pool();
	*tuple_mem = reinterpret_cast<Tuple*>(tuple_mem_);
	*tuple_row_mem = batch_->GetRow(batch_->AddRow());
	return batch_->capacity() - batch_->num_rows();
}

bool CsvScanNode::WriteCompleteTuple(MemPool* pool, std::string line,
    Tuple* tuple, TupleRow* tuple_row, uint8_t* error_fields,
    uint8_t* error_in_row) {
  *error_in_row = false;
  // Initialize tuple before materializing slots
  InitTuple(tuple);

  tokenizer<escaped_list_separator<char> > tok(line);

  int i = 0;
  for(tokenizer<escaped_list_separator<char> >::iterator beg=tok.begin(); beg!=tok.end();++beg){
//  for (int i = 0; i < materialized_slots().size(); ++i) {

    int len = (*beg).length();
//    int len = fields[i].len;
//    if (UNLIKELY(len < 0)) {
//      len = -len;
//    }

    SlotDescriptor* desc = materialized_slots_[i];
    std::string token(*beg);
    bool error = !WriteSlot(desc, tuple, token.c_str(), len, pool);
    error_fields[i] = error;
    *error_in_row |= error;
    i++;
  }

  tuple_row->SetTuple(tuple_idx(), tuple);
  Expr* const* conjuncts = &conjuncts_[0];
  return ExecNode::EvalConjuncts(conjuncts, conjuncts_.size(), tuple_row);
}

int CsvScanNode::WriteAlignedTuples(MemPool* pool, TupleRow* tuple_row,
    int row_size, int num_tuples, int max_added_tuples,
    int slots_per_tuple, int row_idx_start, bool* eos) {

  DCHECK(tuple_ != NULL);
  uint8_t* tuple_row_mem = reinterpret_cast<uint8_t*>(tuple_row);
  uint8_t* tuple_mem = reinterpret_cast<uint8_t*>(tuple_);
  Tuple* tuple = reinterpret_cast<Tuple*>(tuple_mem);

  uint8_t error[slots_per_tuple];
  memset(error, 0, sizeof(error));

  int tuples_returned = 0;

  // Loop through the file and materialize all the tuples
  std::string line;
  while (getline(csv_file_, line)) {
  // for (int i = 0; i < num_tuples; ++i) {
    uint8_t error_in_row = false;
    // Materialize a single tuple.  This function will be replaced by a codegen'd
    // function.
    if (WriteCompleteTuple(pool, line, tuple, tuple_row, error,
          &error_in_row)) {
      ++tuples_returned;
      tuple_mem += tuple_desc_->byte_size();
      tuple_row_mem += row_size;
      tuple = reinterpret_cast<Tuple*>(tuple_mem);
      tuple_row = reinterpret_cast<TupleRow*>(tuple_row_mem);
    }

    if (tuples_returned == max_added_tuples) {
      *eos = false;
      break;
    }
    *eos = true;
  }

  return tuples_returned;
}

int CsvScanNode::WriteFields(MemPool* pool, TupleRow* tuple_row,
            int num_tuples, bool* eos) {
  int num_tuples_processed = 0;
  int num_tuples_materialized = 0;
  if (num_tuples > 0) {
    int max_added_tuples = (limit() == -1) ?
          num_tuples : limit() - rows_returned();
    int tuples_returned = 0;
    tuples_returned = WriteAlignedTuples(pool, tuple_row,
        batch_->row_byte_size(), num_tuples, max_added_tuples,
        materialized_slots_.size(), num_tuples_processed, eos);
    if (tuples_returned == -1) return 0;

    num_tuples_materialized += tuples_returned;
  }
  return num_tuples_materialized;
}

Status CsvScanNode::GetNext(RuntimeState* state, RowBatch* row_batch,
		bool* eos) {
        MemPool* pool;
        TupleRow* tuple_row_mem;
        int max_tuples = GetMemory(&pool, &tuple_, &tuple_row_mem);
        // TODO: do something with max_tuples
        max_tuples++;
        int num_tuples = WriteFields(pool, tuple_row_mem, batch_->capacity(),
            eos);
        if (num_tuples <= 0) *eos = true;
        num_rows_ += num_tuples;
        if (*eos) {
            RETURN_IF_ERROR(CommitLastRows(num_rows_));
            Status status = GetNextInternal(state, row_batch, eos);
            if (status.IsMemLimitExceeded())
                    state->SetMemLimitExceeded();
            *eos = true;
            SetDone();
            return status;
        } else if (num_rows_ == batch_->capacity()) {
            RETURN_IF_ERROR(CommitRows(num_rows_));
            max_tuples = GetMemory(&pool, &tuple_, &tuple_row_mem);
            num_rows_ = 0;
            Status status = GetNextInternal(state, row_batch, eos);
            if (status.IsMemLimitExceeded())
                    state->SetMemLimitExceeded();
            return status;
        }
	return Status::OK;
}

Status CsvScanNode::GetNextInternal(RuntimeState* state, RowBatch* row_batch,
		bool* eos) {
	RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
	RETURN_IF_CANCELLED(state);
	RETURN_IF_ERROR(state->CheckQueryState());
	SCOPED_TIMER(runtime_profile_->total_time_counter());

	/**
	*By this time, the materialized_row_batches_ must already be populated
	*through a call to either CommitRows() or CommitLastRows(). 
	**/

	RowBatch* materialized_batch = materialized_row_batches_->GetBatch();
	if (materialized_batch != NULL) {
		num_owned_io_buffers_ -= materialized_batch->num_io_buffers();
		row_batch->AcquireState(materialized_batch);
		// Update the number of materialized rows instead of when they are materialized.
		// This means that scanners might process and queue up more rows than are necessary
		// for the limit case but we want to avoid the synchronized writes to
		// num_rows_returned_
		num_rows_returned_ += row_batch->num_rows();
		COUNTER_SET(rows_returned_counter_, num_rows_returned_);

		if (ReachedLimit()) {
			int num_rows_over = num_rows_returned_ - limit_;
			row_batch->set_num_rows(row_batch->num_rows() - num_rows_over);
			num_rows_returned_ -= num_rows_over;
			COUNTER_SET(rows_returned_counter_, num_rows_returned_);

			*eos = true;
			SetDone();
		}
		DCHECK_EQ(materialized_batch->num_io_buffers(), 0);
		delete materialized_batch;
		*eos = false;
		return Status::OK;
	}

	*eos = true;
	return Status::OK;
}

void CsvScanNode::Close(RuntimeState* state) {
	if (is_closed())
		return;
	SetDone();
        csv_file_.close();

	//housekeeping tasks
	num_owned_io_buffers_ -= materialized_row_batches_->Cleanup();
	DCHECK_EQ(num_owned_io_buffers_, 0) << "ScanNode has leaked io buffers";

	ExecNode::Close(state);
}

// Do not change this method
void CsvScanNode::SetDone() {
	{
		unique_lock<mutex> l(lock_);
		if (done_)
			return;
		done_ = true;
	}
	materialized_row_batches_->Shutdown();
}

  // Commit num_rows to the current row batch.  If this completes the row batch, the
  // row batch is enqueued with the scan node and StartNewRowBatch is called.
  // Returns Status::OK if the query is not cancelled and hasn't exceeded any mem limits.
  // Scanner can call this with 0 rows to flush any pending resources (attached pools
  // and io buffers) to minimize memory consumption.
  // Do not change this method
Status CsvScanNode::CommitRows(int num_rows) {
	DCHECK(batch_ != NULL);
	DCHECK_LE(num_rows, batch_->capacity() - batch_->num_rows());
	batch_->CommitRows(num_rows);
	tuple_mem_ += tuple_desc_->byte_size() * num_rows;

	if (batch_->AtCapacity()) {
		materialized_row_batches_->AddBatch(batch_);
		StartNewRowBatch();
	}

	return Status::OK;
}

// Do not change this method
Status CsvScanNode::CommitLastRows(int num_rows) {
	DCHECK(batch_ != NULL);
	DCHECK_LE(num_rows, batch_->capacity() - batch_->num_rows());
	batch_->CommitRows(num_rows);
	tuple_mem_ += tuple_desc_->byte_size() * num_rows;

	materialized_row_batches_->AddBatch(batch_);
	return Status::OK;
}
