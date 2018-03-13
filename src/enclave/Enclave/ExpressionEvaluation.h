// -*- c-basic-offset: 2; fill-column: 100 -*-

#include <functional>
#include <typeinfo>

#include "Flatbuffers.h"

int printf(const char *fmt, ...);

#define check(test, ...) do {                   \
    bool result = test;                         \
    if (!result) {                              \
      printf(__VA_ARGS__);                      \
      std::exit(1);                                  \
    }                                           \
  } while (0)

#ifndef EXPRESSION_EVALUATION_H
#define EXPRESSION_EVALUATION_H

using namespace edu::berkeley::cs::rise::opaque;

/**
 * Evaluate a binary arithmetic operation on two tuix::Fields. The operation (template parameter
 * Operation) must be a binary function object parameterized on its input type.
 *
 * The left and right Fields are the inputs to the binary operation. They may be temporary pointers
 * invalidated by further writes to builder; this function will not read them after invalidating.
 */
template<typename TuixExpr, template<typename T> class Operation>
flatbuffers::Offset<tuix::Field> eval_binary_arithmetic_op(
  flatbuffers::FlatBufferBuilder &builder,
  const tuix::Field *left,
  const tuix::Field *right) {

  check(left->value_type() == right->value_type(),
        "%s can't operate on values of different types (%s and %s)\n",
        typeid(TuixExpr).name(),
        tuix::EnumNameFieldUnion(left->value_type()),
        tuix::EnumNameFieldUnion(right->value_type()));
  bool result_is_null = left->is_null() || right->is_null();
  switch (left->value_type()) {
  case tuix::FieldUnion_IntegerField:
  {
    auto result = Operation<int32_t>()(
      static_cast<const tuix::IntegerField *>(left->value())->value(),
      static_cast<const tuix::IntegerField *>(right->value())->value());
    // Writing the result invalidates the left and right temporary pointers
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_IntegerField,
      tuix::CreateIntegerField(builder, result).Union(),
      result_is_null);
  }
  case tuix::FieldUnion_LongField:
  {
    auto result = Operation<int64_t>()(
      static_cast<const tuix::LongField *>(left->value())->value(),
      static_cast<const tuix::LongField *>(right->value())->value());
    // Writing the result invalidates the left and right temporary pointers
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_LongField,
      tuix::CreateLongField(builder, result).Union(),
      result_is_null);
  }
  case tuix::FieldUnion_FloatField:
  {
    auto result = Operation<float>()(
      static_cast<const tuix::FloatField *>(left->value())->value(),
      static_cast<const tuix::FloatField *>(right->value())->value());
    // Writing the result invalidates the left and right temporary pointers
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_FloatField,
      tuix::CreateFloatField(builder, result).Union(),
      result_is_null);
  }
  case tuix::FieldUnion_DoubleField:
  {
    auto result = Operation<double>()(
      static_cast<const tuix::DoubleField *>(left->value())->value(),
      static_cast<const tuix::DoubleField *>(right->value())->value());
    // Writing the result invalidates the left and right temporary pointers
    return tuix::CreateField(
      builder,
      tuix::FieldUnion_DoubleField,
      tuix::CreateDoubleField(builder, result).Union(),
      result_is_null);
  }
  default:
    printf("Can't evaluate %s on %s\n",
           typeid(TuixExpr).name(),
           tuix::EnumNameFieldUnion(left->value_type()));
    std::exit(1);
    return flatbuffers::Offset<tuix::Field>();
  }
}

/**
 * Evaluate a binary number comparison operation on two tuix::Fields. The operation (template
 * parameter Operation) must be a binary function object parameterized on its input type.
 *
 * The left and right Fields are the inputs to the binary operation. They may be temporary pointers
 * invalidated by further writes to builder; this function will not read them after invalidating.
 */
template<typename TuixExpr, template<typename T> class Operation>
flatbuffers::Offset<tuix::Field> eval_binary_comparison(
  flatbuffers::FlatBufferBuilder &builder,
  const tuix::Field *left,
  const tuix::Field *right) {

  check(left->value_type() == right->value_type(),
        "%s can't operate on values of different types (%s and %s)\n",
        typeid(TuixExpr).name(),
        tuix::EnumNameFieldUnion(left->value_type()),
        tuix::EnumNameFieldUnion(right->value_type()));
  bool result_is_null = left->is_null() || right->is_null();
  bool result = false;
  if (!result_is_null) {
    switch (left->value_type()) {
    case tuix::FieldUnion_IntegerField:
    {
      result = Operation<int32_t>()(
        static_cast<const tuix::IntegerField *>(left->value())->value(),
        static_cast<const tuix::IntegerField *>(right->value())->value());
      break;
    }
    case tuix::FieldUnion_LongField:
    {
      result = Operation<int64_t>()(
        static_cast<const tuix::LongField *>(left->value())->value(),
        static_cast<const tuix::LongField *>(right->value())->value());
      break;
    }
    case tuix::FieldUnion_FloatField:
    {
      result = Operation<float>()(
        static_cast<const tuix::FloatField *>(left->value())->value(),
        static_cast<const tuix::FloatField *>(right->value())->value());
      break;
    }
    case tuix::FieldUnion_DoubleField:
    {
      result = Operation<double>()(
        static_cast<const tuix::DoubleField *>(left->value())->value(),
        static_cast<const tuix::DoubleField *>(right->value())->value());
      break;
    }
    case tuix::FieldUnion_StringField:
    {
      auto field1 = static_cast<const tuix::StringField *>(left->value());
      auto field2 = static_cast<const tuix::StringField *>(right->value());
      std::string str1(reinterpret_cast<const char *>(field1->value()->data()), field1->length());
      std::string str2(reinterpret_cast<const char *>(field2->value()->data()), field2->length());
      result = Operation<std::string>()(str1, str2);
      break;
    }
    default:
      printf("Can't evaluate %s on %s\n",
             typeid(TuixExpr).name(),
             tuix::EnumNameFieldUnion(left->value_type()));
      std::exit(1);
    }
  }
  // Writing the result invalidates the left and right temporary pointers
  return tuix::CreateField(
    builder,
    tuix::FieldUnion_BooleanField,
    tuix::CreateBooleanField(builder, result).Union(),
    result_is_null);
}

class FlatbuffersExpressionEvaluator {
public:
  FlatbuffersExpressionEvaluator(const tuix::Expr *expr) : builder(), expr(expr) {}

  /**
   * Evaluate the stored expression on the given row. Return a Field containing the result.
   * Warning: The Field points to internally-managed memory that may be overwritten the next time
   * eval is called. Therefore it is only valid until the next call to eval.
   */
  const tuix::Field *eval(const tuix::Row *row) {
    builder.Clear();
    flatbuffers::Offset<tuix::Field> result_offset = eval_helper(row, expr);
    return flatbuffers::GetTemporaryPointer<tuix::Field>(builder, result_offset);
  }

private:
  /**
   * Evaluate the given expression on the given row. Return the offset (within builder) of the Field
   * containing the result. This offset is only valid until the next call to eval.
   */
  flatbuffers::Offset<tuix::Field> eval_helper(const tuix::Row *row, const tuix::Expr *expr) {
    switch (expr->expr_type()) {
    case tuix::ExprUnion_Col:
    {
      uint32_t col_num = static_cast<const tuix::Col *>(expr->expr())->col_num();
      return flatbuffers_copy<tuix::Field>(
        row->field_values()->Get(col_num), builder);
    }

    case tuix::ExprUnion_Literal:
    {
      return flatbuffers_copy<tuix::Field>(
        static_cast<const tuix::Literal *>(expr->expr())->value(), builder);
    }

    case tuix::ExprUnion_Cast:
    {
      auto cast = static_cast<const tuix::Cast *>(expr->expr());
      // Note: This temporary pointer will be invalidated when we next write to builder
      const tuix::Field *value =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, cast->value()));
      bool result_is_null = value->is_null();
      switch (value->value_type()) {
      case tuix::FieldUnion_IntegerField:
      {
        return flatbuffers_cast<tuix::IntegerField, int32_t>(cast, value, builder, result_is_null);
      }
      case tuix::FieldUnion_LongField:
      {
        return flatbuffers_cast<tuix::LongField, int64_t>(cast, value, builder, result_is_null);
      }
      case tuix::FieldUnion_FloatField:
      {
        return flatbuffers_cast<tuix::FloatField, float>(cast, value, builder, result_is_null);
      }
      case tuix::FieldUnion_DoubleField:
      {
        return flatbuffers_cast<tuix::DoubleField, double>(cast, value, builder, result_is_null);
      }
      case tuix::FieldUnion_DateField:
      {
        return flatbuffers_cast<tuix::DateField, Date>(cast, value, builder, result_is_null);
      }
      default:
      {
        printf("Can't evaluate cast on %s\n",
               tuix::EnumNameFieldUnion(value->value_type()));
        std::exit(1);
        return flatbuffers::Offset<tuix::Field>();
      }
      }
    }

    // Arithmetic
    case tuix::ExprUnion_Add:
    {
      auto add = static_cast<const tuix::Add *>(expr->expr());
      return eval_binary_arithmetic_op<tuix::Add, std::plus>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, add->left())),
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, add->right())));
    }

    case tuix::ExprUnion_Subtract:
    {
      auto subtract = static_cast<const tuix::Subtract *>(expr->expr());
      return eval_binary_arithmetic_op<tuix::Subtract, std::minus>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, subtract->left())),
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, subtract->right())));
    }

    case tuix::ExprUnion_Multiply:
    {
      auto multiply = static_cast<const tuix::Multiply *>(expr->expr());
      return eval_binary_arithmetic_op<tuix::Multiply, std::multiplies>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, multiply->left())),
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, multiply->right())));
    }

    case tuix::ExprUnion_Divide:
    {
      auto divide = static_cast<const tuix::Divide *>(expr->expr());
      return eval_binary_arithmetic_op<tuix::Divide, std::divides>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, divide->left())),
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, divide->right())));
    }

    // Predicates
    case tuix::ExprUnion_And:
    {
      auto a = static_cast<const tuix::And *>(expr->expr());
      auto left = flatbuffers::GetTemporaryPointer(builder, eval_helper(row, a->left()));
      auto right = flatbuffers::GetTemporaryPointer(builder, eval_helper(row, a->right()));

      check(left->value_type() == tuix::FieldUnion_BooleanField
            && right->value_type() == tuix::FieldUnion_BooleanField,
            "And can't operate on %s and %s\n",
            tuix::EnumNameFieldUnion(left->value_type()),
            tuix::EnumNameFieldUnion(right->value_type()));

      bool result = false, result_is_null = false;
      bool left_value = static_cast<const tuix::BooleanField *>(left->value())->value();
      if (!left->is_null() && !left_value) {
        result = false;
      } else {
        bool right_value = static_cast<const tuix::BooleanField *>(right->value())->value();
        if (!right->is_null() && !right_value) {
          result = false;
        } else {
          if (!left->is_null() && !right->is_null()) {
            result = true;
          } else {
            result_is_null = true;
          }
        }
      }

      // Writing the result invalidates the left and right temporary pointers
      return tuix::CreateField(
        builder,
        tuix::FieldUnion_BooleanField,
        tuix::CreateBooleanField(builder, result).Union(),
        result_is_null);
    }

    case tuix::ExprUnion_Or:
    {
      auto o = static_cast<const tuix::Or *>(expr->expr());
      auto left = flatbuffers::GetTemporaryPointer(builder, eval_helper(row, o->left()));
      auto right = flatbuffers::GetTemporaryPointer(builder, eval_helper(row, o->right()));

      check(left->value_type() == tuix::FieldUnion_BooleanField
            && right->value_type() == tuix::FieldUnion_BooleanField,
            "Or can't operate on %s and %s\n",
            tuix::EnumNameFieldUnion(left->value_type()),
            tuix::EnumNameFieldUnion(right->value_type()));

      bool result = false, result_is_null = false;
      bool left_value = static_cast<const tuix::BooleanField *>(left->value())->value();
      if (!left->is_null() && left_value) {
        result = true;
      } else {
        bool right_value = static_cast<const tuix::BooleanField *>(right->value())->value();
        if (!right->is_null() && right_value) {
          result = true;
        } else {
          if (!left->is_null() && !right->is_null()) {
            result = false;
          } else {
            result_is_null = true;
          }
        }
      }

      // Writing the result invalidates the left and right temporary pointers
      return tuix::CreateField(
        builder,
        tuix::FieldUnion_BooleanField,
        tuix::CreateBooleanField(builder, result).Union(),
        result_is_null);
    }

    case tuix::ExprUnion_Not:
    {
      auto n = static_cast<const tuix::Not *>(expr->expr());
      auto child = flatbuffers::GetTemporaryPointer(builder, eval_helper(row, n->child()));

      check(child->value_type() == tuix::FieldUnion_BooleanField,
            "Not can't operate on %s\n",
            tuix::EnumNameFieldUnion(child->value_type()));

      bool child_value = static_cast<const tuix::BooleanField *>(child->value())->value();
      bool result = !child_value, result_is_null = child->is_null();

      // Writing the result invalidates the child temporary pointer
      return tuix::CreateField(
        builder,
        tuix::FieldUnion_BooleanField,
        tuix::CreateBooleanField(builder, result).Union(),
        result_is_null);
    }

    case tuix::ExprUnion_LessThan:
    {
      auto lt = static_cast<const tuix::LessThan *>(expr->expr());
      return eval_binary_comparison<tuix::LessThan, std::less>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, lt->left())),
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, lt->right())));
    }

    case tuix::ExprUnion_LessThanOrEqual:
    {
      auto le = static_cast<const tuix::LessThanOrEqual *>(expr->expr());
      return eval_binary_comparison<tuix::LessThanOrEqual, std::less_equal>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, le->left())),
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, le->right())));
    }

    case tuix::ExprUnion_GreaterThan:
    {
      auto gt = static_cast<const tuix::GreaterThan *>(expr->expr());
      return eval_binary_comparison<tuix::GreaterThan, std::greater>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, gt->left())),
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, gt->right())));
    }

    case tuix::ExprUnion_GreaterThanOrEqual:
    {
      auto ge = static_cast<const tuix::GreaterThanOrEqual *>(expr->expr());
      return eval_binary_comparison<tuix::GreaterThanOrEqual, std::greater_equal>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, ge->left())),
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, ge->right())));
    }

    case tuix::ExprUnion_EqualTo:
    {
      auto eq = static_cast<const tuix::EqualTo *>(expr->expr());
      return eval_binary_comparison<tuix::EqualTo, std::equal_to>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, eq->left())),
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, eq->right())));
    }

    // String expressions
    case tuix::ExprUnion_Substring:
    {
      auto ss = static_cast<const tuix::Substring *>(expr->expr());
      // Note: These temporary pointers will be invalidated when we next write to builder
      const tuix::Field *str =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, ss->str()));
      const tuix::Field *pos =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, ss->pos()));
      const tuix::Field *len =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, ss->len()));
      check(str->value_type() == tuix::FieldUnion_StringField &&
            pos->value_type() == tuix::FieldUnion_IntegerField &&
            len->value_type() == tuix::FieldUnion_IntegerField,
            "tuix::Substring requires str String, pos Integer, len Integer, not "
            "str %s, pos %s, len %s)\n",
            tuix::EnumNameFieldUnion(str->value_type()),
            tuix::EnumNameFieldUnion(pos->value_type()),
            tuix::EnumNameFieldUnion(len->value_type()));
      bool result_is_null = str->is_null() || pos->is_null() || len->is_null();
      if (!result_is_null) {
        auto str_field = static_cast<const tuix::StringField *>(str->value());
        auto pos_val = static_cast<const tuix::IntegerField *>(pos->value())->value();
        auto len_val = static_cast<const tuix::IntegerField *>(len->value())->value();

        // Note that the pos argument of SQL substring is 1-indexed. This logic mirrors that of
        // Spark's common/unsafe/src/main/java/org/apache/spark/unsafe/types/ByteArray.java.
        // TODO: oblivious string lengths
        int32_t start = 0;
        int32_t end;
        if (pos_val > 0) {
          start = pos_val - 1;
        } else if (pos_val < 0) {
          start = str_field->length() + pos_val;
        }
        if ((static_cast<int32_t>(str_field->length()) - start) < len_val) {
          end = str_field->length();
        } else {
          end = start + len_val;
        }
        start = std::max(start, 0);
        if (start > end) {
          start = end;
        }
        std::vector<uint8_t> substring(
          flatbuffers::VectorIterator<uint8_t, uint8_t>(str_field->value()->Data(),
                                                        static_cast<uint32_t>(start)),
          flatbuffers::VectorIterator<uint8_t, uint8_t>(str_field->value()->Data(),
                                                        static_cast<uint32_t>(end)));
        // Writing the result invalidates the str, pos, len temporary pointers
        return tuix::CreateField(
          builder,
          tuix::FieldUnion_StringField,
          tuix::CreateStringFieldDirect(
            builder, &substring, static_cast<uint32_t>(end - start)).Union(),
          result_is_null);
      } else {
        // Writing the result invalidates the str, pos, len temporary pointers
        // TODO: oblivious string lengths
        return tuix::CreateField(
          builder,
          tuix::FieldUnion_StringField,
          tuix::CreateStringFieldDirect(builder, nullptr, 0).Union(),
          result_is_null);
      }
    }

    case tuix::ExprUnion_Contains:
    {
      auto c = static_cast<const tuix::Contains *>(expr->expr());

      // TODO: handle Contains(str, "")

      // Note: These temporary pointers will be invalidated when we next write to builder
      const tuix::Field *left =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, c->left()));
      const tuix::Field *right =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, c->right()));

      check(left->value_type() == tuix::FieldUnion_StringField &&
            right->value_type() == tuix::FieldUnion_StringField &&
            "tuix::Contains requires left String, right String, not"
            "left %s, right %s\n",
            tuix::EnumNameFieldUnion(left->value_type()),
            tuix::EnumNameFieldUnion(right->value_type()));
      bool result_is_null = left->is_null() || right->is_null();
      if (!result_is_null) {
        auto left_field = static_cast<const tuix::StringField *>(left->value());
        auto right_field = static_cast<const tuix::StringField *>(right->value());
        auto last = flatbuffers::VectorIterator<uint8_t, uint8_t>(left_field->value()->Data(), left_field->length());
        auto it = std::find_end(
          flatbuffers::VectorIterator<uint8_t, uint8_t>(left_field->value()->Data(), 0),
          last,
          flatbuffers::VectorIterator<uint8_t, uint8_t>(right_field->value()->Data(), 0),
          flatbuffers::VectorIterator<uint8_t, uint8_t>(right_field->value()->Data(), right_field->length()));
        bool result = (it != last);
        return tuix::CreateField(
          builder,
          tuix::FieldUnion_BooleanField,
          tuix::CreateBooleanField(builder, result).Union(),
          false);
      } else {
        return tuix::CreateField(
          builder,
          tuix::FieldUnion_BooleanField,
          tuix::CreateBooleanField(builder, false).Union(),
          true);
      }
    }

    // Conditional expressions
    case tuix::ExprUnion_If:
    {
      auto e = static_cast<const tuix::If *>(expr->expr());
      // Note: These temporary pointers will be invalidated when we next write to builder
      const tuix::Field *predicate =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, e->predicate()));
      const tuix::Field *true_value =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, e->true_value()));
      const tuix::Field *false_value =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, e->false_value()));
      check(predicate->value_type() == tuix::FieldUnion_BooleanField,
            "tuix::If requires predicate to return Boolean, not %s\n",
            tuix::EnumNameFieldUnion(predicate->value_type()));
      check(true_value->value_type() == false_value->value_type(),
            "tuix::If requires true and false types to be the same, but %s != %s\n",
            tuix::EnumNameFieldUnion(true_value->value_type()),
            tuix::EnumNameFieldUnion(false_value->value_type()));
      if (!predicate->is_null()) {
        bool pred_val = static_cast<const tuix::BooleanField *>(predicate->value())->value();
        if (pred_val) {
          return GetOffset<tuix::Field>(builder, true_value);
        } else {
          return GetOffset<tuix::Field>(builder, false_value);
        }
      } else {
        // Writing the result invalidates the predicate, true_value, false_value temporary pointers
        // TODO: this is therefore unsafe
        return flatbuffers_copy<tuix::Field>(true_value, builder, true);
      }
    }

    // Null expressions
    case tuix::ExprUnion_IsNull:
    {
      auto e = static_cast<const tuix::IsNull *>(expr->expr());
      // Note: This temporary pointer will be invalidated when we next write to builder
      const tuix::Field *child =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, e->child()));
      bool result = child->is_null();
      return tuix::CreateField(
        builder,
        tuix::FieldUnion_BooleanField,
        tuix::CreateBooleanField(builder, result).Union(),
        false);
    }

    default:
      printf("Can't evaluate expression of type %s\n",
             tuix::EnumNameExprUnion(expr->expr_type()));
      std::exit(1);
      return flatbuffers::Offset<tuix::Field>();
    }
  }

  flatbuffers::FlatBufferBuilder builder;
  const tuix::Expr *expr;
};

class FlatbuffersSortOrderEvaluator {
public:
  FlatbuffersSortOrderEvaluator(const tuix::SortExpr *sort_expr)
    : sort_expr(sort_expr), builder() {
    for (auto sort_order_it = sort_expr->sort_order()->begin();
         sort_order_it != sort_expr->sort_order()->end(); ++sort_order_it) {
      sort_order_evaluators.emplace_back(
        std::unique_ptr<FlatbuffersExpressionEvaluator>(
          new FlatbuffersExpressionEvaluator(sort_order_it->child())));
    }
  }

  FlatbuffersSortOrderEvaluator(uint8_t *buf, size_t len) {
    flatbuffers::Verifier v(buf, len);
    check(v.VerifyBuffer<tuix::SortExpr>(nullptr),
          "Corrupt SortExpr %p of length %d\n", buf, len);
    sort_expr = flatbuffers::GetRoot<tuix::SortExpr>(buf);

    for (auto sort_order_it = sort_expr->sort_order()->begin();
         sort_order_it != sort_expr->sort_order()->end(); ++sort_order_it) {
      sort_order_evaluators.emplace_back(
        std::unique_ptr<FlatbuffersExpressionEvaluator>(
          new FlatbuffersExpressionEvaluator(sort_order_it->child())));
    }
  }

  bool less_than(const tuix::Row *row1, const tuix::Row *row2) {
    builder.Clear();
    const tuix::Row *a = nullptr, *b = nullptr;
    for (uint32_t i = 0; i < sort_order_evaluators.size(); i++) {
      switch (sort_expr->sort_order()->Get(i)->direction()) {
      case tuix::SortDirection_Ascending:
        a = row1;
        b = row2;
        break;
      case tuix::SortDirection_Descending:
        a = row2;
        b = row1;
        break;
      }

      const tuix::Field *a_eval_tmp = sort_order_evaluators[i]->eval(a);
      const tuix::Field *a_eval = flatbuffers::GetTemporaryPointer<tuix::Field>(
        builder, flatbuffers_copy(a_eval_tmp, builder));
      const tuix::Field *b_eval_tmp = sort_order_evaluators[i]->eval(b);
      const tuix::Field *b_eval = flatbuffers::GetTemporaryPointer<tuix::Field>(
        builder, flatbuffers_copy(b_eval_tmp, builder));

      bool a_less_than_b =
        static_cast<const tuix::BooleanField *>(
          flatbuffers::GetTemporaryPointer<tuix::Field>(
            builder,
            eval_binary_comparison<tuix::LessThan, std::less>(
              builder, a_eval, b_eval))
          ->value())->value();
      bool b_less_than_a =
        static_cast<const tuix::BooleanField *>(
          flatbuffers::GetTemporaryPointer<tuix::Field>(
            builder,
            eval_binary_comparison<tuix::LessThan, std::less>(
              builder, b_eval, a_eval))
          ->value())->value();

      if (a_less_than_b) {
        return true;
      } else if (b_less_than_a) {
        return false;
      }
    }
    return false;
  }

private:
  const tuix::SortExpr *sort_expr;
  flatbuffers::FlatBufferBuilder builder;
  std::vector<std::unique_ptr<FlatbuffersExpressionEvaluator>> sort_order_evaluators;
};

class FlatbuffersJoinExprEvaluator {
public:
  FlatbuffersJoinExprEvaluator(uint8_t *buf, size_t len)
    : builder() {
    flatbuffers::Verifier v(buf, len);
    check(v.VerifyBuffer<tuix::JoinExpr>(nullptr),
          "Corrupt JoinExpr %p of length %d\n", buf, len);

    const tuix::JoinExpr* join_expr = flatbuffers::GetRoot<tuix::JoinExpr>(buf);

    check(join_expr->left_keys()->size() == join_expr->right_keys()->size(),
          "Mismatched join key lengths\n");
    for (auto key_it = join_expr->left_keys()->begin();
         key_it != join_expr->left_keys()->end(); ++key_it) {
      left_key_evaluators.emplace_back(
        std::unique_ptr<FlatbuffersExpressionEvaluator>(
          new FlatbuffersExpressionEvaluator(*key_it)));
    }
    for (auto key_it = join_expr->right_keys()->begin();
         key_it != join_expr->right_keys()->end(); ++key_it) {
      right_key_evaluators.emplace_back(
        std::unique_ptr<FlatbuffersExpressionEvaluator>(
          new FlatbuffersExpressionEvaluator(*key_it)));
    }
  }

  /**
   * Return true if the given row is from the primary table, indicated by its first field, which
   * must be an IntegerField.
   */
  bool is_primary(const tuix::Row *row) {
    return static_cast<const tuix::IntegerField *>(
      row->field_values()->Get(0)->value())->value() == 0;
  }

  /** Return true if the two rows are from the same join group. */
  bool is_same_group(const tuix::Row *row1, const tuix::Row *row2) {
    auto &row1_evaluators = is_primary(row1) ? left_key_evaluators : right_key_evaluators;
    auto &row2_evaluators = is_primary(row2) ? left_key_evaluators : right_key_evaluators;

    builder.Clear();
    for (uint32_t i = 0; i < row1_evaluators.size(); i++) {
      const tuix::Field *row1_eval_tmp = row1_evaluators[i]->eval(row1);
      const tuix::Field *row1_eval = flatbuffers::GetTemporaryPointer<tuix::Field>(
        builder, flatbuffers_copy(row1_eval_tmp, builder));
      const tuix::Field *row2_eval_tmp = row2_evaluators[i]->eval(row2);
      const tuix::Field *row2_eval = flatbuffers::GetTemporaryPointer<tuix::Field>(
        builder, flatbuffers_copy(row2_eval_tmp, builder));

      bool row1_equals_row2 =
        static_cast<const tuix::BooleanField *>(
          flatbuffers::GetTemporaryPointer<tuix::Field>(
            builder,
            eval_binary_comparison<tuix::EqualTo, std::equal_to>(
              builder, row1_eval, row2_eval))
          ->value())->value();

      if (!row1_equals_row2) {
        return false;
      }
    }
    return true;
  }

private:
  flatbuffers::FlatBufferBuilder builder;
  std::vector<std::unique_ptr<FlatbuffersExpressionEvaluator>> left_key_evaluators;
  std::vector<std::unique_ptr<FlatbuffersExpressionEvaluator>> right_key_evaluators;
};

class AggregateExpressionEvaluator {
public:
  AggregateExpressionEvaluator(const tuix::AggregateExpr *expr) : builder() {
    for (auto initial_value_expr : *expr->initial_values()) {
      initial_value_evaluators.emplace_back(
        std::unique_ptr<FlatbuffersExpressionEvaluator>(
          new FlatbuffersExpressionEvaluator(initial_value_expr)));
    }
    for (auto update_expr : *expr->update_exprs()) {
      update_evaluators.emplace_back(
        std::unique_ptr<FlatbuffersExpressionEvaluator>(
          new FlatbuffersExpressionEvaluator(update_expr)));
    }
    evaluate_evaluator.reset(new FlatbuffersExpressionEvaluator(expr->evaluate_expr()));
  }

  std::vector<const tuix::Field *> initial_values(const tuix::Row *unused) {
    std::vector<const tuix::Field *> result;
    for (auto&& e : initial_value_evaluators) {
      result.push_back(e->eval(unused));
    }
    return result;
  }

  std::vector<const tuix::Field *> update(const tuix::Row *concat) {
    std::vector<const tuix::Field *> result;
    for (auto&& e : update_evaluators) {
      result.push_back(e->eval(concat));
    }
    return result;
  }

  const tuix::Field *evaluate(const tuix::Row *agg) {
    return evaluate_evaluator->eval(agg);
  }

private:
  flatbuffers::FlatBufferBuilder builder;
  std::vector<std::unique_ptr<FlatbuffersExpressionEvaluator>> initial_value_evaluators;
  std::vector<std::unique_ptr<FlatbuffersExpressionEvaluator>> update_evaluators;
  std::unique_ptr<FlatbuffersExpressionEvaluator> evaluate_evaluator;
};

class FlatbuffersAggOpEvaluator {
public:
  FlatbuffersAggOpEvaluator(uint8_t *buf, size_t len)
    : a(nullptr), builder(), builder2() {
    flatbuffers::Verifier v(buf, len);
    check(v.VerifyBuffer<tuix::AggregateOp>(nullptr),
          "Corrupt AggregateOp %p of length %d\n", buf, len);

    const tuix::AggregateOp* agg_op = flatbuffers::GetRoot<tuix::AggregateOp>(buf);

    for (auto e : *agg_op->grouping_expressions()) {
      grouping_evaluators.emplace_back(
        std::unique_ptr<FlatbuffersExpressionEvaluator>(
          new FlatbuffersExpressionEvaluator(e)));
    }
    for (auto e : *agg_op->aggregate_expressions()) {
      aggregate_evaluators.emplace_back(
        std::unique_ptr<AggregateExpressionEvaluator>(
          new AggregateExpressionEvaluator(e)));
    }

    reset_group();
  }

  void reset_group() {
    builder2.Clear();
    // Write initial values to a
    std::vector<flatbuffers::Offset<tuix::Field>> init_fields;
    for (auto&& e : aggregate_evaluators) {
      for (auto f : e->initial_values(nullptr)) {
        init_fields.push_back(flatbuffers_copy<tuix::Field>(f, builder2));
      }
    }
    a = flatbuffers::GetTemporaryPointer<tuix::Row>(
      builder2, tuix::CreateRowDirect(builder2, &init_fields));
  }

  void set(const tuix::Row *agg_row) {
    builder2.Clear();
    if (agg_row) {
      a = flatbuffers::GetTemporaryPointer<tuix::Row>(
        builder2, flatbuffers_copy<tuix::Row>(agg_row, builder2));
    } else {
      reset_group();
    }
  }

  void aggregate(const tuix::Row *row) {
    builder.Clear();
    flatbuffers::Offset<tuix::Row> concat;

    std::vector<flatbuffers::Offset<tuix::Field>> concat_fields;
    // concat row to a
    for (auto field : *a->field_values()) {
      concat_fields.push_back(flatbuffers_copy<tuix::Field>(field, builder));
    }
    for (auto field : *row->field_values()) {
      concat_fields.push_back(flatbuffers_copy<tuix::Field>(field, builder));
    }
    concat = tuix::CreateRowDirect(builder, &concat_fields);
    const tuix::Row *concat_ptr = flatbuffers::GetTemporaryPointer<tuix::Row>(builder, concat);

    // run update_exprs
    builder2.Clear();
    std::vector<flatbuffers::Offset<tuix::Field>> output_fields;
    for (auto&& e : aggregate_evaluators) {
      for (auto f : e->update(concat_ptr)) {
        output_fields.push_back(flatbuffers_copy<tuix::Field>(f, builder2));
      }
    }
    a = flatbuffers::GetTemporaryPointer<tuix::Row>(
      builder2, tuix::CreateRowDirect(builder2, &output_fields));
  }

  const tuix::Row *get_partial_agg() {
    return a;
  }

  const tuix::Row *evaluate() {
    builder.Clear();
    std::vector<flatbuffers::Offset<tuix::Field>> output_fields;
    for (auto&& e : aggregate_evaluators) {
      output_fields.push_back(flatbuffers_copy<tuix::Field>(e->evaluate(a), builder));
    }
    return flatbuffers::GetTemporaryPointer<tuix::Row>(
      builder,
      tuix::CreateRowDirect(builder, &output_fields));
  }

  /** Return true if the two rows are from the same join group. */
  bool is_same_group(const tuix::Row *row1, const tuix::Row *row2) {
    builder.Clear();
    for (auto it = grouping_evaluators.begin(); it != grouping_evaluators.end(); ++it) {
      const tuix::Field *row1_eval_tmp = (*it)->eval(row1);
      const tuix::Field *row1_eval = flatbuffers::GetTemporaryPointer<tuix::Field>(
        builder, flatbuffers_copy(row1_eval_tmp, builder));
      const tuix::Field *row2_eval_tmp = (*it)->eval(row2);
      const tuix::Field *row2_eval = flatbuffers::GetTemporaryPointer<tuix::Field>(
        builder, flatbuffers_copy(row2_eval_tmp, builder));

      bool row1_equals_row2 =
        static_cast<const tuix::BooleanField *>(
          flatbuffers::GetTemporaryPointer<tuix::Field>(
            builder,
            eval_binary_comparison<tuix::EqualTo, std::equal_to>(
              builder, row1_eval, row2_eval))
          ->value())->value();

      if (!row1_equals_row2) {
        return false;
      }
    }
    return true;
  }

private:
  // Pointer into builder2
  const tuix::Row *a;

  flatbuffers::FlatBufferBuilder builder;
  flatbuffers::FlatBufferBuilder builder2;
  std::vector<std::unique_ptr<FlatbuffersExpressionEvaluator>> grouping_evaluators;
  std::vector<std::unique_ptr<AggregateExpressionEvaluator>> aggregate_evaluators;
};

#endif
