#include "Project.h"

#include "ExpressionEvaluation.h"
#include "common.h"

void project(uint8_t *project_list, size_t project_list_length,
             uint8_t *input_rows, uint32_t input_rows_length,
             uint8_t **output_rows, uint32_t *output_rows_length) {
  flatbuffers::Verifier v(project_list, project_list_length);
  check(v.VerifyBuffer<tuix::ProjectExpr>(nullptr),
        "Corrupt ProjectExpr %p of length %d\n", project_list, project_list_length);

  // Create a vector of expression evaluators, one per output column
  const tuix::ProjectExpr* project_expr =
    flatbuffers::GetRoot<tuix::ProjectExpr>(project_list);
  std::vector<std::unique_ptr<FlatbuffersExpressionEvaluator>> project_eval_list;
  for (auto it = project_expr->project_list()->begin();
       it != project_expr->project_list()->end();
       ++it) {
    project_eval_list.emplace_back(new FlatbuffersExpressionEvaluator(*it));
  }

  EncryptedBlockToRowReader r(input_rows, input_rows_length);
  FlatbuffersRowWriter w;

  std::vector<const tuix::Field *> out_fields(project_eval_list.size());

  for (auto it = r.begin(); it != r.end(); ++it) {
    for (uint32_t j = 0; j < project_eval_list.size(); j++) {
      out_fields[j] = project_eval_list[j]->eval(*it);
    }

    w.write(out_fields);
  }

  w.close();
  *output_rows = w.output_buffer();
  *output_rows_length = w.output_size();
}
