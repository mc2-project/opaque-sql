// -*- c-basic-offset: 2; fill-column: 100 -*-

#include <functional>
#include <typeinfo>
#include <cmath>
#include <limits>

#include "Flatbuffers.h"

int printf(const char *fmt, ...);

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

  if (left->value_type() != right->value_type()) {
    throw std::runtime_error(
      std::string(typeid(TuixExpr).name())
      + std::string(" can't operate on values of different types (")
      + std::string(tuix::EnumNameFieldUnion(left->value_type()))
      + std::string(" and ")
      + std::string(tuix::EnumNameFieldUnion(right->value_type()))
      + std::string(")"));
  }

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
    throw std::runtime_error(
      std::string("Can't evaluate ")
      + std::string(typeid(TuixExpr).name())
      + std::string(" on ")
      + std::string(tuix::EnumNameFieldUnion(left->value_type())));
  }
}

/**
 * Evaluate a binary number comparison operation on two tuix::Fields. The operation (template
 * parameter Operation) must be a binary function object parameterized on its input type.
 *
 * The left and right Fields are the inputs to the binary operation. They may be temporary pointers
 * invalidated by further writes to builder; this function will not read them after invalidating.
 * 
 * When either of the values is NULL, this function will return a NULL value, with a boolean underlying 
 * value that is used for internal functions like sorting. 
 * The parameter `nulls_first` enables the function to order either by NULLS FIRST or by NULLS LAST.
 */
template<typename TuixExpr, template<typename T> class Operation>
flatbuffers::Offset<tuix::Field> eval_binary_comparison(
  flatbuffers::FlatBufferBuilder &builder,
  const tuix::Field *left,
  const tuix::Field *right,
  bool nulls_first = true) {

  if (left->value_type() != right->value_type()) {
    throw std::runtime_error(
      std::string(typeid(TuixExpr).name())
      + std::string(" can't operate on values of different types (")
      + std::string(tuix::EnumNameFieldUnion(left->value_type()))
      + std::string(" and ")
      + std::string(tuix::EnumNameFieldUnion(right->value_type()))
      + std::string(")"));
  }

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
    case tuix::FieldUnion_DateField:
    {
      result = Operation<int32_t>()(
        static_cast<const tuix::DateField *>(left->value())->value(),
        static_cast<const tuix::DateField *>(right->value())->value());
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
    case tuix::FieldUnion_ArrayField:
    {
      auto vector1 = left->value_as_ArrayField()->value();
      auto vector2 = right->value_as_ArrayField()->value();

      std::vector<double> vector1_packed;
      std::vector<double> vector2_packed;

      for (flatbuffers::uoffset_t i = 0; i < vector1->size(); ++i) {
        if (vector1->Get(i)->value_type() != tuix::FieldUnion_DoubleField) {
          throw std::runtime_error(
            std::string("For comparison, only Array[Double] is supported, but array contained ")
            + std::string(tuix::EnumNameFieldUnion(vector1->Get(i)->value_type())));
        }

        vector1_packed.push_back(vector1->Get(i)->value_as_DoubleField()->value());
      }

      for (flatbuffers::uoffset_t i = 0; i < vector2->size(); ++i) {
        if (vector2->Get(i)->value_type() != tuix::FieldUnion_DoubleField) {
          throw std::runtime_error(
            std::string("For comparison, only Array[Double] is supported, but array contained ")
            + std::string(tuix::EnumNameFieldUnion(vector2->Get(i)->value_type())));
        }

        vector2_packed.push_back(vector2->Get(i)->value_as_DoubleField()->value());
      }

      result = Operation<std::vector<double>>()(vector1_packed, vector2_packed);
      break;
    }
    default:
      throw std::runtime_error(
        std::string("Can't evaluate ")
        + std::string(typeid(TuixExpr).name())
        + std::string(" on ")
        + std::string(tuix::EnumNameFieldUnion(left->value_type())));
    }
  } else {
    // This code block handles comparison when at least one value is NULL.
    // The logic can be summarized as: (note that the != operation implements XOR for boolean values)
    //
    // If nulls_first = 1
    // | Input values | is_null() value | Inputs to Operation |
    // | (x, NULL)    | (0, 1)          | (1, 0)              |
    // | (NULL, x)    | (1, 0)          | (0, 1)              |
    // | (NULL, NULL) | (1, 1)          | (0, 0)              |
    //
    // A similar table can be derived for when nulls_first is false
    
    bool left_is_null = left->is_null() != nulls_first;
    bool right_is_null = right->is_null() != nulls_first;
    result = Operation<bool>()(left_is_null, right_is_null);
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
      auto * literal = static_cast<const tuix::Literal *>(expr->expr());
      const tuix::Field *value = literal->value();

      // If type is CalendarInterval, manually return a calendar interval field. 
      // Otherwise 'days' disappears in conversion.
      if (value->value_type() == tuix::FieldUnion_CalendarIntervalField) {

        auto  *interval = value->value_as_CalendarIntervalField();
        uint32_t months = interval->months();
        uint32_t days = interval->days();
        uint64_t ms = interval->microseconds();

        return tuix::CreateField(
          builder,
          tuix::FieldUnion_CalendarIntervalField,
          tuix::CreateCalendarIntervalField(builder, months, days, ms).Union(),
          false);
      }

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
      case tuix::FieldUnion_StringField:
      {
        auto sf = value->value_as_StringField();
        std::string s(sf->value()->begin(), sf->value()->begin() + sf->length());
        switch (cast->target_type()) {
        case tuix::ColType_IntegerType:
        {
          uint32_t result = 0;
          if (!result_is_null) {
            result = std::stol(s);
          }
          return tuix::CreateField(
            builder,
            tuix::FieldUnion_IntegerField,
            tuix::CreateIntegerField(builder, result).Union(),
            result_is_null);
        }
        case tuix::ColType_LongType:
        {
          uint64_t result = 0;
          if (!result_is_null) {
            result = std::stoll(s);
          }
          return tuix::CreateField(
            builder,
            tuix::FieldUnion_LongField,
            tuix::CreateLongField(builder, result).Union(),
            result_is_null);
        }
        case tuix::ColType_FloatType:
        {
          float result = 0;
          if (!result_is_null) {
            result = std::stof(s);
          }
          return tuix::CreateField(
            builder,
            tuix::FieldUnion_FloatField,
            tuix::CreateFloatField(builder, result).Union(),
            result_is_null);
        }
        case tuix::ColType_DoubleType:
        {
          double result = 0;
          if (!result_is_null) {
            result = std::stod(s);
          }
          return tuix::CreateField(
            builder,
            tuix::FieldUnion_DoubleField,
            tuix::CreateDoubleField(builder, result).Union(),
            result_is_null);
        }
        default:
          throw std::runtime_error(
            std::string("Can't cast String to ")
            + std::string(tuix::EnumNameColType(cast->target_type())));
        }
      }
      case tuix::FieldUnion_ArrayField:
      {
        if (cast->target_type() != tuix::ColType_StringType) {
          throw std::runtime_error(
            std::string("Can't cast Array to ")
            + std::string(tuix::EnumNameColType(cast->target_type()))
            + std::string(", only StringType"));
        }
        auto array_field = static_cast<const tuix::ArrayField *>(value->value());
        std::string str = to_string(array_field);
        std::vector<uint8_t> str_vec(str.begin(), str.end());
        return tuix::CreateField(
          builder,
          tuix::FieldUnion_StringField,
          tuix::CreateStringFieldDirect(builder, &str_vec, str_vec.size()).Union(),
          result_is_null);
      }
      case tuix::FieldUnion_MapField:
      {
        if (cast->target_type() != tuix::ColType_StringType) {
          throw std::runtime_error(
            std::string("Can't cast Map to ")
            + std::string(tuix::EnumNameColType(cast->target_type()))
            + std::string(", only StringType"));
        }
        auto map_field = static_cast<const tuix::MapField *>(value->value());
        std::string str = to_string(map_field);
        std::vector<uint8_t> str_vec(str.begin(), str.end());
        return tuix::CreateField(
          builder,
          tuix::FieldUnion_StringField,
          tuix::CreateStringFieldDirect(builder, &str_vec, str_vec.size()).Union(),
          result_is_null);
      }
      default:
        throw std::runtime_error(
          std::string("Can't evaluate cast on ")
          + std::string(tuix::EnumNameFieldUnion(value->value_type())));
      }
    }

    // Arithmetic
    case tuix::ExprUnion_Add:
    {
      auto add = static_cast<const tuix::Add *>(expr->expr());
      auto left_offset = eval_helper(row, add->left());
      auto right_offset = eval_helper(row, add->right());

      return eval_binary_arithmetic_op<tuix::Add, std::plus>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, left_offset),
        flatbuffers::GetTemporaryPointer(builder, right_offset));
    }

    case tuix::ExprUnion_Subtract:
    {
      auto subtract = static_cast<const tuix::Subtract *>(expr->expr());
      auto left_offset = eval_helper(row, subtract->left());
      auto right_offset = eval_helper(row, subtract->right());
      return eval_binary_arithmetic_op<tuix::Subtract, std::minus>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, left_offset),
        flatbuffers::GetTemporaryPointer(builder, right_offset));
    }

    case tuix::ExprUnion_Multiply:
    {
      auto multiply = static_cast<const tuix::Multiply *>(expr->expr());
      auto left_offset = eval_helper(row, multiply->left());
      auto right_offset = eval_helper(row, multiply->right());
      return eval_binary_arithmetic_op<tuix::Multiply, std::multiplies>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, left_offset),
        flatbuffers::GetTemporaryPointer(builder, right_offset));
    }

    case tuix::ExprUnion_Divide:
    {
      auto divide = static_cast<const tuix::Divide *>(expr->expr());
      auto left_offset = eval_helper(row, divide->left());
      auto right_offset = eval_helper(row, divide->right());
      return eval_binary_arithmetic_op<tuix::Divide, std::divides>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, left_offset),
        flatbuffers::GetTemporaryPointer(builder, right_offset));
    }

    // Predicates
    case tuix::ExprUnion_And:
    {
      auto a = static_cast<const tuix::And *>(expr->expr());
      auto left_offset = eval_helper(row, a->left());
      auto right_offset = eval_helper(row, a->right());
      auto left = flatbuffers::GetTemporaryPointer(builder, left_offset);
      auto right = flatbuffers::GetTemporaryPointer(builder, right_offset);

      if (left->value_type() != tuix::FieldUnion_BooleanField
          || right->value_type() != tuix::FieldUnion_BooleanField) {
        throw std::runtime_error(
          std::string("And can't operate on ")
          + std::string(tuix::EnumNameFieldUnion(left->value_type()))
          + std::string(" and ")
          + std::string(tuix::EnumNameFieldUnion(right->value_type())));
      }

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
      auto left_offset = eval_helper(row, o->left());
      auto right_offset = eval_helper(row, o->right());
      auto left = flatbuffers::GetTemporaryPointer(builder, left_offset);
      auto right = flatbuffers::GetTemporaryPointer(builder, right_offset);

      if (left->value_type() != tuix::FieldUnion_BooleanField
          || right->value_type() != tuix::FieldUnion_BooleanField) {
        throw std::runtime_error(
          std::string("Or can't operate on ")
          + std::string(tuix::EnumNameFieldUnion(left->value_type()))
          + std::string(" and ")
          + std::string(tuix::EnumNameFieldUnion(right->value_type())));
      }

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

      if (child->value_type() != tuix::FieldUnion_BooleanField) {
        throw std::runtime_error(
          std::string("Not can't operate on ")
          + std::string(tuix::EnumNameFieldUnion(child->value_type())));
      }

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
      auto left_offset = eval_helper(row, lt->left());
      auto right_offset = eval_helper(row, lt->right());
      return eval_binary_comparison<tuix::LessThan, std::less>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, left_offset),
        flatbuffers::GetTemporaryPointer(builder, right_offset));
    }

    case tuix::ExprUnion_LessThanOrEqual:
    {
      auto le = static_cast<const tuix::LessThanOrEqual *>(expr->expr());
      auto left_offset = eval_helper(row, le->left());
      auto right_offset = eval_helper(row, le->right());
      return eval_binary_comparison<tuix::LessThanOrEqual, std::less_equal>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, left_offset),
        flatbuffers::GetTemporaryPointer(builder, right_offset));
    }

    case tuix::ExprUnion_GreaterThan:
    {
      auto gt = static_cast<const tuix::GreaterThan *>(expr->expr());
      auto left_offset = eval_helper(row, gt->left());
      auto right_offset = eval_helper(row, gt->right());
      return eval_binary_comparison<tuix::GreaterThan, std::greater>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, left_offset),
        flatbuffers::GetTemporaryPointer(builder, right_offset));
    }

    case tuix::ExprUnion_GreaterThanOrEqual:
    {
      auto ge = static_cast<const tuix::GreaterThanOrEqual *>(expr->expr());
      auto left_offset = eval_helper(row, ge->left());
      auto right_offset = eval_helper(row, ge->right());
      return eval_binary_comparison<tuix::GreaterThanOrEqual, std::greater_equal>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, left_offset),
        flatbuffers::GetTemporaryPointer(builder, right_offset));
    }

    case tuix::ExprUnion_EqualTo:
    {
      auto eq = static_cast<const tuix::EqualTo *>(expr->expr());
      auto left_offset = eval_helper(row, eq->left());
      auto right_offset = eval_helper(row, eq->right());
      return eval_binary_comparison<tuix::EqualTo, std::equal_to>(
        builder,
        flatbuffers::GetTemporaryPointer(builder, left_offset),
        flatbuffers::GetTemporaryPointer(builder, right_offset));
    }

    // String expressions
    case tuix::ExprUnion_Substring:
    {
      auto ss = static_cast<const tuix::Substring *>(expr->expr());
      auto str_offset = eval_helper(row, ss->str());
      auto pos_offset = eval_helper(row, ss->pos());
      auto len_offset = eval_helper(row, ss->len());
      // Note: These temporary pointers will be invalidated when we next write to builder
      const tuix::Field *str = flatbuffers::GetTemporaryPointer(builder, str_offset);
      const tuix::Field *pos = flatbuffers::GetTemporaryPointer(builder, pos_offset);
      const tuix::Field *len = flatbuffers::GetTemporaryPointer(builder, len_offset);
      if (str->value_type() != tuix::FieldUnion_StringField
          || pos->value_type() != tuix::FieldUnion_IntegerField
          || len->value_type() != tuix::FieldUnion_IntegerField) {
        throw std::runtime_error(
          std::string("tuix::Substring requires str String, pos Integer, len Integer, not ")
          + std::string("str ")
          + std::string(tuix::EnumNameFieldUnion(str->value_type()))
          + std::string(", pos ")
          + std::string(tuix::EnumNameFieldUnion(pos->value_type()))
          + std::string(", val ")
          + std::string(tuix::EnumNameFieldUnion(len->value_type())));
      }
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
      // TODO: handle Contains(str, "")

      auto c = static_cast<const tuix::Contains *>(expr->expr());
      auto left_offset = eval_helper(row, c->left());
      auto right_offset = eval_helper(row, c->right());

      // Note: These temporary pointers will be invalidated when we next write to builder
      const tuix::Field *left = flatbuffers::GetTemporaryPointer(builder, left_offset);
      const tuix::Field *right = flatbuffers::GetTemporaryPointer(builder, right_offset);

      if (left->value_type() != tuix::FieldUnion_StringField
          || right->value_type() != tuix::FieldUnion_StringField) {
        throw std::runtime_error(
          std::string("tuix::Contains requires left String, right String, not ")
          + std::string("left ")
          + std::string(tuix::EnumNameFieldUnion(left->value_type()))
          + std::string(", right ")
          + std::string(tuix::EnumNameFieldUnion(right->value_type())));
      }

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

    case tuix::ExprUnion_Upper:
    {
      auto n = static_cast<const tuix::Upper *>(expr->expr());
      auto child_offset = eval_helper(row, n->child());
      const tuix::Field *str = flatbuffers::GetTemporaryPointer(builder, child_offset);

      if (str->value_type() != tuix::FieldUnion_StringField) {
        throw std::runtime_error(
          std::string("tuix::Upper requires str String, not ")
          + std::string("str ")
          + std::string(tuix::EnumNameFieldUnion(str->value_type())));
      }

      // Obtain the input as a string
      bool result_is_null = str->is_null();

      if (!result_is_null) {

        auto str_field = static_cast<const tuix::StringField *>(str->value());

        std::vector<uint8_t> str_vec(
            flatbuffers::VectorIterator<uint8_t, uint8_t>(str_field->value()->Data(),
                                                          static_cast<uint32_t>(0)),
            flatbuffers::VectorIterator<uint8_t, uint8_t>(str_field->value()->Data(),
                                                          static_cast<uint32_t>(str_field->length())));

        std::string lower(str_vec.begin(), str_vec.end());

        // Turn lower into uppercase and revert to vector
        std::transform(lower.begin(), lower.end(), lower.begin(), ::toupper);
        std::vector<uint8_t> result(lower.begin(), lower.end());

        // Writing the result
        return tuix::CreateField(
          builder,
          tuix::FieldUnion_StringField,
          tuix::CreateStringFieldDirect(builder, &result, str_field->length()).Union(),
          result_is_null);
      } else {

        // Creation is failing with null pointer. Trivially create empty string
        std::string empty("\0");
        std::vector<uint8_t> result(empty.begin(), empty.end());

        return tuix::CreateField(
          builder,
          tuix::FieldUnion_StringField,
          tuix::CreateStringFieldDirect(builder, &result, 0).Union(),
          result_is_null);
      }
    }

    case tuix::ExprUnion_Like:
    {
      auto e = static_cast<const tuix::Like *>(expr->expr());
      auto left_offset = eval_helper(row, e->left());
      auto right_offset = eval_helper(row, e->right());
      const tuix::Field *left = flatbuffers::GetTemporaryPointer(builder, left_offset);
      const tuix::Field *right = flatbuffers::GetTemporaryPointer(builder, right_offset);
      
      // Type check
      if (left->value_type() != tuix::FieldUnion_StringField
          || right->value_type() != tuix::FieldUnion_StringField) {
        throw std::runtime_error(
          std::string("tuix::Contains requires left String, right String, not ")
          + std::string("left ")
          + std::string(tuix::EnumNameFieldUnion(left->value_type()))
          + std::string(", right ")
          + std::string(tuix::EnumNameFieldUnion(right->value_type())));
      }
      
      // Null check
      if (left->is_null() || right->is_null()) {
        return tuix::CreateField(
          builder,
          tuix::FieldUnion_BooleanField,
          tuix::CreateBooleanField(builder, false).Union(),
          true);
      }

      // Copy strings into char buffer
      auto left_field = static_cast<const tuix::StringField *>(left->value());
      auto right_field = static_cast<const tuix::StringField *>(right->value());
      uint32_t n = left_field->length();
      uint32_t m = right_field->length();

      // DP algorithm for wildcard matching taken from:
      // https://www.geeksforgeeks.org/wildcard-pattern-matching/
      bool result;
      if (m == 0)
        return (n == 0);
      bool lookup[n + 1][m + 1];
      memset(lookup, false, sizeof(lookup));
      lookup[0][0] = true;
      for (uint32_t j = 1; j <= m; j++) {
        if (right_field->value()->Get(j - 1) == '%') {
          lookup[0][j] = lookup[0][j - 1];
        }
      }
      for (uint32_t i = 1; i <= n; i++) {
        for (uint32_t j = 1; j <= m; j++) {
          if (right_field->value()->Get(j - 1) == '%') {
            lookup[i][j] = lookup[i][j - 1] || lookup[i - 1][j];
          } else if (right_field->value()->Get(j - 1) == '_' || 
                     left_field->value()->Get(i - 1) == right_field->value()->Get(j - 1)) {
            lookup[i][j] = lookup[i - 1][j - 1];
          } else {
            lookup[i][j] = false;
          }
        }
      }
      result = lookup[n][m];
      return tuix::CreateField(
        builder,
        tuix::FieldUnion_BooleanField,
        tuix::CreateBooleanField(builder, result).Union(),
        false);
    }

    case tuix::ExprUnion_StartsWith:
    {
      auto e = static_cast<const tuix::Like *>(expr->expr());
      auto left_offset = eval_helper(row, e->left());
      auto right_offset = eval_helper(row, e->right());
      const tuix::Field *left = flatbuffers::GetTemporaryPointer(builder, left_offset);
      const tuix::Field *right = flatbuffers::GetTemporaryPointer(builder, right_offset);
      
      // Type check
      if (left->value_type() != tuix::FieldUnion_StringField
          || right->value_type() != tuix::FieldUnion_StringField) {
        throw std::runtime_error(
          std::string("tuix::Contains requires left String, right String, not ")
          + std::string("left ")
          + std::string(tuix::EnumNameFieldUnion(left->value_type()))
          + std::string(", right ")
          + std::string(tuix::EnumNameFieldUnion(right->value_type())));
      }
      
      // Null check
      if (left->is_null() || right->is_null()) {
        return tuix::CreateField(
          builder,
          tuix::FieldUnion_BooleanField,
          tuix::CreateBooleanField(builder, false).Union(),
          true);
      }

      auto left_field = static_cast<const tuix::StringField *>(left->value());
      auto right_field = static_cast<const tuix::StringField *>(right->value());
      uint32_t pattern_len = right_field->length();
      bool result = true;
      for (uint32_t i = 0; i < pattern_len; i++) {
        result = result && (left_field->value()->Get(i) == right_field->value()->Get(i));
      }
      return tuix::CreateField(
        builder,
        tuix::FieldUnion_BooleanField,
        tuix::CreateBooleanField(builder, result).Union(),
        false);
    }

    case tuix::ExprUnion_EndsWith:
    {
      auto e = static_cast<const tuix::Like *>(expr->expr());
      auto left_offset = eval_helper(row, e->left());
      auto right_offset = eval_helper(row, e->right());
      const tuix::Field *left = flatbuffers::GetTemporaryPointer(builder, left_offset);
      const tuix::Field *right = flatbuffers::GetTemporaryPointer(builder, right_offset);
      
      // Type check
      if (left->value_type() != tuix::FieldUnion_StringField
          || right->value_type() != tuix::FieldUnion_StringField) {
        throw std::runtime_error(
          std::string("tuix::Contains requires left String, right String, not ")
          + std::string("left ")
          + std::string(tuix::EnumNameFieldUnion(left->value_type()))
          + std::string(", right ")
          + std::string(tuix::EnumNameFieldUnion(right->value_type())));
      }
      
      // Null check
      if (left->is_null() || right->is_null()) {
        return tuix::CreateField(
          builder,
          tuix::FieldUnion_BooleanField,
          tuix::CreateBooleanField(builder, false).Union(),
          true);
      }

      auto left_field = static_cast<const tuix::StringField *>(left->value());
      auto right_field = static_cast<const tuix::StringField *>(right->value());
      uint32_t str_len = left_field->length();
      uint32_t pattern_len = right_field->length();
      bool result = true;
      for (uint32_t i = 0; i < pattern_len; i++) {
        uint32_t str_dex = str_len - pattern_len + i;
        result = result && (left_field->value()->Get(str_dex) == right_field->value()->Get(i));
      }
      return tuix::CreateField(
        builder,
        tuix::FieldUnion_BooleanField,
        tuix::CreateBooleanField(builder, result).Union(),
        false);
    }

    // Conditional expressions
    case tuix::ExprUnion_If:
    {
      auto e = static_cast<const tuix::If *>(expr->expr());
      auto predicate_offset = eval_helper(row, e->predicate());
      auto true_value_offset = eval_helper(row, e->true_value());
      auto false_value_offset = eval_helper(row, e->false_value());
      // Note: These temporary pointers will be invalidated when we next write to builder
      const tuix::Field *predicate =
        flatbuffers::GetTemporaryPointer(builder, predicate_offset);
      const tuix::Field *true_value =
        flatbuffers::GetTemporaryPointer(builder, true_value_offset);
      const tuix::Field *false_value =
        flatbuffers::GetTemporaryPointer(builder, false_value_offset);
      if (predicate->value_type() != tuix::FieldUnion_BooleanField) {
        throw std::runtime_error(
          std::string("tuix::If requires predicate to return Boolean, not ")
          + std::string(tuix::EnumNameFieldUnion(predicate->value_type())));
      }
      if (true_value->value_type() != false_value->value_type()) {
        throw std::runtime_error(
          std::string("tuix::If requires true and false types to be the same, but ")
          + std::string(tuix::EnumNameFieldUnion(true_value->value_type()))
          + std::string(" != ")
          + std::string(tuix::EnumNameFieldUnion(false_value->value_type())));
      }
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

    case tuix::ExprUnion_CaseWhen:
    {
      auto e = expr->expr_as_CaseWhen();
      size_t num_children = e->children()->size();

      // Evaluate to the first value whose predicate is true.
      // Short circuit on the earliest branch possible.
      tuix::FieldUnion result_type = tuix::FieldUnion_NONE;
      for (size_t i = 0; i < num_children - 1; i += 2) {
        auto predicate_offset = eval_helper(row, (*e->children())[i]);
        auto true_value_offset = eval_helper(row, (*e->children())[i+1]);
        const tuix::Field *predicate =
          flatbuffers::GetTemporaryPointer(builder, predicate_offset);
        const tuix::Field *true_value =
          flatbuffers::GetTemporaryPointer(builder, true_value_offset);
        if (result_type == tuix::FieldUnion_NONE) {
          result_type = true_value->value_type();
        }
        if (predicate->value_type() != tuix::FieldUnion_BooleanField) {
          throw std::runtime_error(
            std::string("tuix::CaseWhen requires predicate to return Boolean, not ")
            + std::string(tuix::EnumNameFieldUnion(predicate->value_type())));
        }
        if (true_value->value_type() != result_type) {
          throw std::runtime_error(
            std::string("tuix::CaseWhen requires a uniform data type, but ")
            + std::string(tuix::EnumNameFieldUnion(true_value->value_type()))
            + std::string(" != ")
            + std::string(tuix::EnumNameFieldUnion(result_type)));
        }
        if (!predicate->is_null()) {
          bool pred_val = static_cast<const tuix::BooleanField *>(predicate->value())->value();
          if (pred_val) {
            return GetOffset<tuix::Field>(builder, true_value);
          }
        }
      }

      // None of the predicates were true.
      // Return the else value if it exists, or a null value if it doesn't.
      if (num_children % 2 == 1) {
        auto else_value_offset = eval_helper(row, (*e->children())[num_children-1]);
        const tuix::Field *else_value = flatbuffers::GetTemporaryPointer(builder, else_value_offset);
        return GetOffset<tuix::Field>(builder, else_value);
      }
      // Null strings require special handling...
      if (result_type == tuix::FieldUnion_StringField) {
        std::string empty("\0");
        std::vector<uint8_t> result(empty.begin(), empty.end());
        return tuix::CreateField(
          builder,
          tuix::FieldUnion_StringField,
          tuix::CreateStringFieldDirect(builder, &result, 0).Union(),
          true);
      }
      return tuix::CreateField(builder, result_type,
          tuix::CreateNullField(builder).Union(), true);
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

    // Time expressions
    case tuix::ExprUnion_DateAdd:
    {
      auto c = static_cast<const tuix::DateAdd *>(expr->expr());
      auto left_offset = eval_helper(row, c->left());
      auto right_offset = eval_helper(row, c->right());

      // Note: These temporary pointers will be invalidated when we next write to builder
      const tuix::Field *left = flatbuffers::GetTemporaryPointer(builder, left_offset);
      const tuix::Field *right = flatbuffers::GetTemporaryPointer(builder, right_offset);

      if (left->value_type() != tuix::FieldUnion_DateField
          || right->value_type() != tuix::FieldUnion_IntegerField) {
          throw std::runtime_error(
          std::string("tuix::DateAdd requires date Date, increment Integer, not ")
          + std::string("date ")
          + std::string(tuix::EnumNameFieldUnion(left->value_type()))
          + std::string(", increment ")
          + std::string(tuix::EnumNameFieldUnion(right->value_type())));
        }

      bool result_is_null = left->is_null() || right->is_null();

      if (!result_is_null) {
        auto left_field = static_cast<const tuix::DateField *>(left->value());
        auto right_field = static_cast<const tuix::IntegerField *>(right->value());

        uint32_t result = left_field->value() + right_field->value();

        return tuix::CreateField(
          builder,
          tuix::FieldUnion_DateField,
          tuix::CreateDateField(builder, result).Union(),
          result_is_null);
      } else {
        uint32_t result = 0;
        return tuix::CreateField(
          builder,
          tuix::FieldUnion_DateField,
          tuix::CreateDateField(builder, result).Union(),
          result_is_null);
      }
    }

    case tuix::ExprUnion_DateAddInterval:
    {
      auto c = static_cast<const tuix::DateAddInterval *>(expr->expr());
      auto left_offset = eval_helper(row, c->left());
      auto right_offset = eval_helper(row, c->right());

      // Note: These temporary pointers will be invalidated when we next write to builder
      const tuix::Field *left = flatbuffers::GetTemporaryPointer(builder, left_offset);
      const tuix::Field *right = flatbuffers::GetTemporaryPointer(builder, right_offset);

      if (left->value_type() != tuix::FieldUnion_DateField
          || right->value_type() != tuix::FieldUnion_CalendarIntervalField) {
          throw std::runtime_error(
          std::string("tuix::DateAddInterval requires date Date, interval CalendarIntervalField, not ")
          + std::string("date ")
          + std::string(tuix::EnumNameFieldUnion(left->value_type()))
          + std::string(", interval ")
          + std::string(tuix::EnumNameFieldUnion(right->value_type())));
        }

      bool result_is_null = left->is_null() || right->is_null();
      uint32_t result = 0;

      if (!result_is_null) {

        auto left_field = static_cast<const tuix::DateField *>(left->value());
        auto right_field = static_cast<const tuix::CalendarIntervalField *>(right->value());

        //This is an approximation
        //TODO take into account leap seconds
        uint64_t date = 86400L*left_field->value();
        struct tm tm;
        secs_to_tm(date, &tm);
        tm.tm_mon += right_field->months();
        tm.tm_mday += right_field->days();
        time_t time = std::mktime(&tm);
        uint32_t result = (time + (right_field->microseconds() / 1000)) / 86400L;

        return tuix::CreateField(
          builder,
          tuix::FieldUnion_DateField,
          tuix::CreateDateField(builder, result).Union(),
          result_is_null);
      } else {
        return tuix::CreateField(
          builder,
          tuix::FieldUnion_DateField,
          tuix::CreateDateField(builder, result).Union(),
          result_is_null);
      }
    }

    case tuix::ExprUnion_Year:
    {
      auto e = static_cast<const tuix::Year *>(expr->expr());
      // Note: This temporary pointer will be invalidated when we next write to builder
      const tuix::Field *child =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, e->child()));
      if (child->value_type() != tuix::FieldUnion_DateField) {
        throw std::runtime_error(
          std::string("tuix::Year requires child Date, not ")
          + std::string(tuix::EnumNameFieldUnion(child->value_type())));
      }
      uint32_t result = 0;
      bool child_is_null = child->is_null();
      if (!child_is_null) {
        auto child_field = static_cast<const tuix::DateField *>(child->value());
        //This is an approximation
        //TODO take into account leap seconds
        uint64_t date = 86400L*child_field->value();
        struct tm tm;
        secs_to_tm(date, &tm);
        result = 1900 + tm.tm_year;
      }
      return tuix::CreateField(
        builder,
        tuix::FieldUnion_IntegerField,
        tuix::CreateIntegerField(builder, result).Union(),
        child_is_null);
    }

    // Math expressions
    case tuix::ExprUnion_Exp:
    {
      auto e = static_cast<const tuix::Exp *>(expr->expr());
      auto child = flatbuffers::GetTemporaryPointer(builder, eval_helper(row, e->child()));

      if (child->value_type() != tuix::FieldUnion_DoubleField) {
        throw std::runtime_error(
          std::string("Exp can't operate on ")
          + std::string(tuix::EnumNameFieldUnion(child->value_type())));
      }

      auto x = child->value_as_DoubleField()->value();
      double result = 0.0;
      bool result_is_null = child->is_null();
      if (!result_is_null) {
        result = std::exp(x);
      }

      return tuix::CreateField(
        builder,
        tuix::FieldUnion_DoubleField,
        tuix::CreateDoubleField(builder, result).Union(),
        result_is_null);
    }

    // Complex type creation
    case tuix::ExprUnion_CreateArray:
    {
      auto e = expr->expr_as_CreateArray();

      std::vector<flatbuffers::Offset<tuix::Field>> children_offsets;
      for (auto child_expr : *e->children()) {
        children_offsets.push_back(eval_helper(row, child_expr));
      }

      return tuix::CreateField(
        builder,
        tuix::FieldUnion_ArrayField,
        tuix::CreateArrayFieldDirect(builder, &children_offsets).Union(),
        false);
    }

    // Opaque UDFs
    case tuix::ExprUnion_VectorAdd:
    {
      auto e = static_cast<const tuix::VectorAdd *>(expr->expr());
      auto left_offset = eval_helper(row, e->left());
      auto right_offset = eval_helper(row, e->right());
      // Note: These temporary pointers will be invalidated when we next write to builder
      auto left = flatbuffers::GetTemporaryPointer(builder, left_offset);
      auto right = flatbuffers::GetTemporaryPointer(builder, right_offset);

      if (left->value_type() != tuix::FieldUnion_ArrayField
          || right->value_type() != tuix::FieldUnion_ArrayField) {
        throw std::runtime_error(
          std::string("VectorAdd can't operate on ")
          + std::string(tuix::EnumNameFieldUnion(left->value_type()))
          + std::string(" and ")
          + std::string(tuix::EnumNameFieldUnion(right->value_type())));
      }

      auto v1 = left->value_as_ArrayField()->value();
      auto v2 = right->value_as_ArrayField()->value();

      std::vector<double> result_values;
      bool result_is_null = left->is_null() || right->is_null();
      if (!result_is_null) {
        flatbuffers::uoffset_t size = std::max(v1->size(), v2->size());
        for (flatbuffers::uoffset_t i = 0; i < size; ++i) {
          if (i < v1->size() && v1->Get(i)->value_type() != tuix::FieldUnion_DoubleField) {
            throw std::runtime_error(
              std::string("VectorAdd expected Array[Double], but the left array contained ")
              + std::string(tuix::EnumNameFieldUnion(v1->Get(i)->value_type())));
          }
          if (i < v2->size() && v2->Get(i)->value_type() != tuix::FieldUnion_DoubleField) {
            throw std::runtime_error(
              std::string("VectorAdd expected Array[Double], but the right array contained ")
              + std::string(tuix::EnumNameFieldUnion(v2->Get(i)->value_type())));
          }

          double v1_i = i < v1->size() ? v1->Get(i)->value_as_DoubleField()->value() : 0.0;
          double v2_i = i < v2->size() ? v2->Get(i)->value_as_DoubleField()->value() : 0.0;

          result_values.push_back(v1_i + v2_i);
        }
      }

      std::vector<flatbuffers::Offset<tuix::Field>> result;
      for (double result_i : result_values) {
        result.push_back(
          tuix::CreateField(
            builder,
            tuix::FieldUnion_DoubleField,
            tuix::CreateDoubleField(builder, result_i).Union(),
            false));
      }
      return tuix::CreateField(
        builder,
        tuix::FieldUnion_ArrayField,
        tuix::CreateArrayFieldDirect(builder, &result).Union(),
        result_is_null);
    }

    case tuix::ExprUnion_VectorMultiply:
    {
      auto e = static_cast<const tuix::VectorMultiply *>(expr->expr());
      auto left_offset = eval_helper(row, e->left());
      auto right_offset = eval_helper(row, e->right());
      // Note: These temporary pointers will be invalidated when we next write to builder
      auto left = flatbuffers::GetTemporaryPointer(builder, left_offset);
      auto right = flatbuffers::GetTemporaryPointer(builder, right_offset);

      if (left->value_type() != tuix::FieldUnion_ArrayField
          || right->value_type() != tuix::FieldUnion_DoubleField) {
        throw std::runtime_error(
          std::string("VectorMultiply can't operate on ")
          + std::string(tuix::EnumNameFieldUnion(left->value_type()))
          + std::string(" and ")
          + std::string(tuix::EnumNameFieldUnion(right->value_type())));
      }

      auto v = left->value_as_ArrayField()->value();
      double c = right->value_as_DoubleField()->value();

      std::vector<double> result_values;
      bool result_is_null = left->is_null() || right->is_null();
      if (!result_is_null) {
        for (flatbuffers::uoffset_t i = 0; i < v->size(); ++i) {
          if (v->Get(i)->value_type() != tuix::FieldUnion_DoubleField) {
            throw std::runtime_error(
              std::string("VectorMultiply expected Array[Double], but the array contained ")
              + std::string(tuix::EnumNameFieldUnion(v->Get(i)->value_type())));
          }
          double v_i = v->Get(i)->value_as_DoubleField()->value();

          result_values.push_back(v_i * c);
        }
      }

      std::vector<flatbuffers::Offset<tuix::Field>> result;
      for (double result_i : result_values) {
        result.push_back(
          tuix::CreateField(
            builder,
            tuix::FieldUnion_DoubleField,
            tuix::CreateDoubleField(builder, result_i).Union(),
            false));
      }
      return tuix::CreateField(
        builder,
        tuix::FieldUnion_ArrayField,
        tuix::CreateArrayFieldDirect(builder, &result).Union(),
        result_is_null);
    }

    case tuix::ExprUnion_DotProduct:
    {
      auto e = static_cast<const tuix::DotProduct *>(expr->expr());
      auto left_offset = eval_helper(row, e->left());
      auto right_offset = eval_helper(row, e->right());
      // Note: These temporary pointers will be invalidated when we next write to builder
      auto left = flatbuffers::GetTemporaryPointer(builder, left_offset);
      auto right = flatbuffers::GetTemporaryPointer(builder, right_offset);

      if (left->value_type() != tuix::FieldUnion_ArrayField
          || right->value_type() != tuix::FieldUnion_ArrayField) {
        throw std::runtime_error(
          std::string("DotProduct can't operate on ")
          + std::string(tuix::EnumNameFieldUnion(left->value_type()))
          + std::string(" and ")
          + std::string(tuix::EnumNameFieldUnion(right->value_type())));
      }

      auto v1 = left->value_as_ArrayField()->value();
      auto v2 = right->value_as_ArrayField()->value();

      double result = 0.0;
      bool result_is_null = left->is_null() || right->is_null();
      if (!result_is_null) {
        flatbuffers::uoffset_t size = std::min(v1->size(), v2->size());
        for (flatbuffers::uoffset_t i = 0; i < size; ++i) {
          if (v1->Get(i)->value_type() != tuix::FieldUnion_DoubleField) {
            throw std::runtime_error(
              std::string("VectorMultiply expected Array[Double], but the left array contained ")
              + std::string(tuix::EnumNameFieldUnion(v1->Get(i)->value_type())));
          }
          if (v2->Get(i)->value_type() != tuix::FieldUnion_DoubleField) {
            throw std::runtime_error(
              std::string("VectorMultiply expected Array[Double], but the right array contained ")
              + std::string(tuix::EnumNameFieldUnion(v2->Get(i)->value_type())));
          }

          double v1_i = v1->Get(i)->value_as_DoubleField()->value();
          double v2_i = v2->Get(i)->value_as_DoubleField()->value();

          result += v1_i * v2_i;
        }
      }

      return tuix::CreateField(
        builder,
        tuix::FieldUnion_DoubleField,
        tuix::CreateDoubleField(builder, result).Union(),
        result_is_null);
    }

    case tuix::ExprUnion_ClosestPoint:
    {
      auto e = static_cast<const tuix::ClosestPoint *>(expr->expr());
      auto left_offset = eval_helper(row, e->left());
      auto right_offset = eval_helper(row, e->right());
      // Note: These temporary pointers will be invalidated when we next write to builder
      auto left = flatbuffers::GetTemporaryPointer(builder, left_offset);
      auto right = flatbuffers::GetTemporaryPointer(builder, right_offset);

      if (left->value_type() != tuix::FieldUnion_ArrayField
          || right->value_type() != tuix::FieldUnion_ArrayField) {
        throw std::runtime_error(
          std::string("ClosestPoint can't operate on ")
          + std::string(tuix::EnumNameFieldUnion(left->value_type()))
          + std::string(" and ")
          + std::string(tuix::EnumNameFieldUnion(right->value_type())));
      }

      auto point = left->value_as_ArrayField()->value();
      auto centroids = right->value_as_ArrayField()->value();

      flatbuffers::uoffset_t best_index = 0;
      double best_distance = std::numeric_limits<double>::infinity();

      bool result_is_null = left->is_null() || right->is_null();
      if (!result_is_null) {
        for (flatbuffers::uoffset_t i = 0; i < centroids->size(); ++i) {
          auto centroid_i = centroids->Get(i)->value_as_ArrayField()->value();

          double distance_i = 0.0;

          for (flatbuffers::uoffset_t j = 0; j < point->size() && j < centroid_i->size(); ++j) {
            if (point->Get(j)->value_type() != tuix::FieldUnion_DoubleField) {
              throw std::runtime_error(
                std::string("ClosestPoint expected Array[Double], but points contained ")
                + std::string(tuix::EnumNameFieldUnion(point->Get(j)->value_type())));
            }
            if (centroid_i->Get(j)->value_type() != tuix::FieldUnion_DoubleField) {
              throw std::runtime_error(
                std::string("ClosestPoint expected Array[Double], but a centroid contained ")
                + std::string(tuix::EnumNameFieldUnion(centroid_i->Get(j)->value_type())));
            }

            double point_j = point->Get(j)->value_as_DoubleField()->value();
            double centroid_i_j = centroid_i->Get(j)->value_as_DoubleField()->value();

            double dist = point_j - centroid_i_j;
            distance_i += dist * dist;
          }

          if (distance_i < best_distance) {
            best_distance = distance_i;
            best_index = i;
          }
        }
      }

      std::vector<double> result_values;
      auto result_centroid = centroids->Get(best_index)->value_as_ArrayField()->value();
      for (flatbuffers::uoffset_t i = 0; i < result_centroid->size(); ++i) {
        result_values.push_back(result_centroid->Get(i)->value_as_DoubleField()->value());
      }

      std::vector<flatbuffers::Offset<tuix::Field>> result;
      for (double result_i : result_values) {
        result.push_back(
          tuix::CreateField(
            builder,
            tuix::FieldUnion_DoubleField,
            tuix::CreateDoubleField(builder, result_i).Union(),
            false));
      }
      return tuix::CreateField(
        builder,
        tuix::FieldUnion_ArrayField,
        tuix::CreateArrayFieldDirect(builder, &result).Union(),
        result_is_null);
    }

    default:
      throw std::runtime_error(
        std::string("Can't evaluate expression of type ")
        + std::string(tuix::EnumNameExprUnion(expr->expr_type())));
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
    if (!v.VerifyBuffer<tuix::SortExpr>(nullptr)) {
      throw std::runtime_error(
        std::string("Corrupt SortExpr buffer of length ")
        + std::to_string(len));
    }
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

      // Evaluate the field of comparison for rows a and b. Because the first evaluation returns a
      // temporary pointer that is invalidated by the second evaluation, we must copy the result of
      // the first evaluation before performing the second evaluation. For simplicity, we then
      // maintain offsets into builder rather than pointers, because offsets will not be invalidated
      // by further operations.
      auto a_eval_offset = flatbuffers_copy(sort_order_evaluators[i]->eval(a), builder);
      auto b_eval_offset = flatbuffers_copy(sort_order_evaluators[i]->eval(b), builder);

      // We ignore the result's NULL flag and use only its underlying value for the comparison.
      // eval_binary_comparison() uses this underlying value to pass the desired information.
      bool a_less_than_b =
        static_cast<const tuix::BooleanField *>(
          flatbuffers::GetTemporaryPointer<tuix::Field>(
            builder,
            eval_binary_comparison<tuix::LessThan, std::less>(
              builder,
              flatbuffers::GetTemporaryPointer<tuix::Field>(builder, a_eval_offset),
              flatbuffers::GetTemporaryPointer<tuix::Field>(builder, b_eval_offset)))
          ->value())->value();
      bool b_less_than_a =
        static_cast<const tuix::BooleanField *>(
          flatbuffers::GetTemporaryPointer<tuix::Field>(
            builder,
            eval_binary_comparison<tuix::LessThan, std::less>(
              builder,
              flatbuffers::GetTemporaryPointer<tuix::Field>(builder, b_eval_offset),
              flatbuffers::GetTemporaryPointer<tuix::Field>(builder, a_eval_offset)))
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
    if (!v.VerifyBuffer<tuix::JoinExpr>(nullptr)) {
      throw std::runtime_error(
        std::string("Corrupt JoinExpr buffer of length ")
        + std::to_string(len));
    }

    const tuix::JoinExpr* join_expr = flatbuffers::GetRoot<tuix::JoinExpr>(buf);

    if (join_expr->left_keys()->size() != join_expr->right_keys()->size()) {
      throw std::runtime_error("Mismatched join key lengths");
    }
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
      auto row1_eval_offset = flatbuffers_copy(row1_eval_tmp, builder);
      const tuix::Field *row2_eval_tmp = row2_evaluators[i]->eval(row2);
      auto row2_eval_offset = flatbuffers_copy(row2_eval_tmp, builder);

      bool row1_equals_row2 =
        static_cast<const tuix::BooleanField *>(
          flatbuffers::GetTemporaryPointer<tuix::Field>(
            builder,
            eval_binary_comparison<tuix::EqualTo, std::equal_to>(
              builder,
              flatbuffers::GetTemporaryPointer<tuix::Field>(builder, row1_eval_offset),
              flatbuffers::GetTemporaryPointer<tuix::Field>(builder, row2_eval_offset)))
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
    for (auto eval_expr : *expr->evaluate_exprs()) {
      evaluate_evaluators.emplace_back(
        std::unique_ptr<FlatbuffersExpressionEvaluator>(
          new FlatbuffersExpressionEvaluator(eval_expr)));
    }
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

  std::vector<const tuix::Field *> evaluate(const tuix::Row *agg) {
    std::vector<const tuix::Field *> result;
    for (auto&& e : evaluate_evaluators) {
      result.push_back(e->eval(agg));
    }
    return result;
  }

private:
  flatbuffers::FlatBufferBuilder builder;
  std::vector<std::unique_ptr<FlatbuffersExpressionEvaluator>> initial_value_evaluators;
  std::vector<std::unique_ptr<FlatbuffersExpressionEvaluator>> update_evaluators;
  std::vector<std::unique_ptr<FlatbuffersExpressionEvaluator>> evaluate_evaluators;
};

class FlatbuffersAggOpEvaluator {
public:
  FlatbuffersAggOpEvaluator(uint8_t *buf, size_t len)
    : a(nullptr), builder(), builder2() {
    flatbuffers::Verifier v(buf, len);
    if (!v.VerifyBuffer<tuix::AggregateOp>(nullptr)) {
      throw std::runtime_error(
        std::string("Corrupt AggregateOp buffer of length ")
        + std::to_string(len));
    }
    
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

  size_t get_num_grouping_keys() {
    return grouping_evaluators.size();
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
      for (auto f : e->evaluate(a)) {
        output_fields.push_back(flatbuffers_copy<tuix::Field>(f, builder));
      }
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
      auto row1_eval_offset = flatbuffers_copy(row1_eval_tmp, builder);
      const tuix::Field *row2_eval_tmp = (*it)->eval(row2);
      auto row2_eval_offset = flatbuffers_copy(row2_eval_tmp, builder);

      bool row1_equals_row2 =
        static_cast<const tuix::BooleanField *>(
          flatbuffers::GetTemporaryPointer<tuix::Field>(
            builder,
            eval_binary_comparison<tuix::EqualTo, std::equal_to>(
              builder,
              flatbuffers::GetTemporaryPointer<tuix::Field>(builder, row1_eval_offset),
              flatbuffers::GetTemporaryPointer<tuix::Field>(builder, row2_eval_offset)))
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
