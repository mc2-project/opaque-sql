#include "Aggregate.h"

#include "ExpressionEvaluation.h"
#include "FlatbuffersReaders.h"
#include "FlatbuffersWriters.h"
#include "common.h"

void non_oblivious_aggregate(
  uint8_t *agg_op, size_t agg_op_length,
  uint8_t *input_rows, size_t input_rows_length,
  uint8_t **output_rows, size_t *output_rows_length,
  bool is_partial) {

  FlatbuffersAggOpEvaluator agg_op_eval(agg_op, agg_op_length);
  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  RowWriter w;

  FlatbuffersTemporaryRow prev, cur;
  size_t count = 0;

  while (r.has_next()) {
    prev.set(cur.get());
    cur.set(r.next());
    
    if (prev.get() != nullptr && !agg_op_eval.is_same_group(prev.get(), cur.get())) {
      w.append(agg_op_eval.evaluate());
      agg_op_eval.reset_group();
    }
    agg_op_eval.aggregate(cur.get());
    count += 1;
  }

  // Skip outputting the final row if the number of input rows is 0 AND
  // 1. It's a grouping aggregation, OR
  // 2. It's a global aggregation, the mode is final
  if (!(count == 0 && (agg_op_eval.get_num_grouping_keys() > 0 || (agg_op_eval.get_num_grouping_keys() == 0 && !is_partial)))) {
    w.append(agg_op_eval.evaluate());
  }

  w.output_buffer(output_rows, output_rows_length, std::string("nonObliviousAggregate"));
}

