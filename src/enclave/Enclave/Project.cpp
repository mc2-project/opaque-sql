#include "Project.h"

#include "ExpressionEvaluation.h"
#include "FlatbuffersReaders.h"
#include "FlatbuffersWriters.h"
#include "common.h"

void project(uint8_t *project_list, size_t project_list_length,
             uint8_t *input_rows, size_t input_rows_length,
             uint8_t **output_rows, size_t *output_rows_length) {
  BufferRefView<tuix::ProjectExpr> project_list_buf(project_list, project_list_length);
  project_list_buf.verify();

  // Create a vector of expression evaluators, one per output column
  const tuix::ProjectExpr* project_expr = project_list_buf.root();
  std::vector<std::unique_ptr<FlatbuffersExpressionEvaluator>> project_eval_list;
  for (auto it = project_expr->project_list()->begin();
       it != project_expr->project_list()->end();
       ++it) {
    project_eval_list.emplace_back(new FlatbuffersExpressionEvaluator(*it));
  }

  RowReader r(BufferRefView<tuix::EncryptedBlocks>(input_rows, input_rows_length));
  RowWriter w;

  std::vector<const tuix::Field *> out_fields(project_eval_list.size());

  while (r.has_next()) {
    const tuix::Row *row = r.next();
    for (uint32_t j = 0; j < project_eval_list.size(); j++) {
      out_fields[j] = project_eval_list[j]->eval(row);
    }
    w.append(out_fields);
  }
  
  w.output_buffer(output_rows, output_rows_length, std::string("project"));
}
