// -*- c-basic-offset: 2; fill-column: 100 -*-

#include "util.h"
#include <limits>
#include <set>
#include "EncryptedDAG.h"
#include "EncryptedBlock_generated.h"
#include "Rows_generated.h"
#include "expressions_generated.h"

using namespace edu::berkeley::cs::rise::opaque;

class NewJoinRecord;
class StreamRowReader;
class StreamRowWriter;

template<typename T>
flatbuffers::Offset<T> flatbuffers_copy(
  const T *flatbuffers_obj, flatbuffers::FlatBufferBuilder& builder);
template<>
flatbuffers::Offset<tuix::Row> flatbuffers_copy(
  const tuix::Row *row, flatbuffers::FlatBufferBuilder& builder);
template<>
flatbuffers::Offset<tuix::Field> flatbuffers_copy(
  const tuix::Field *field, flatbuffers::FlatBufferBuilder& builder);

int printf(const char *fmt, ...);

#define check(test, ...) do {                   \
    bool result = test;                         \
    if (!result) {                              \
      printf(__VA_ARGS__);                      \
      assert(result);                           \
    }                                           \
  } while (0)

#ifndef NEW_INTERNAL_TYPES_H
#define NEW_INTERNAL_TYPES_H

bool attrs_equal(const uint8_t *a, const uint8_t *b);

uint32_t copy_attr(uint8_t *dst, const uint8_t *src);

template<typename Type>
uint32_t write_attr(uint8_t *output, Type value, bool dummy);

template<typename Type>
uint32_t read_attr(uint8_t *input, uint8_t *value);

uint32_t read_attr_internal(uint8_t *input, uint8_t *value, uint8_t expected_type);

uint8_t *get_attr_internal(uint8_t *row, uint32_t attr_idx, uint32_t num_cols);

bool attr_less_than(const uint8_t *a, const uint8_t *b);

uint32_t attr_key_prefix(const uint8_t *attr);

/**
 * A standard record (row) in plaintext. Supports reading and writing to and from plaintext and
 * encrypted formats. It can be reused for multiple rows by alternating calls to read and write.
 * It stores row data as bytes in the following format:
 *
 *     [uint32_t num_cols]([uint8_t attr1_type][uint32_t attr1_len][attr1_contents])...
 *
 * Note that num_cols is stored as part of the row data, unlike in the existing codebase.
 */
class NewRecord {
public:
  NewRecord() : NewRecord(ROW_UPPER_BOUND) {}

  NewRecord(uint32_t upper_bound) : row_length(4) {
    row = (uint8_t *) calloc(upper_bound, sizeof(uint8_t));
  }

  ~NewRecord() {
    free(row);
  }

  static void init_dummy(NewRecord *dummy, int op_code);

  /** Delete all attributes from the record. */
  void clear();

  /** Create attributes of the specified types, sizing each to the type's upper bound. */
  void init(const uint8_t *types, uint32_t types_len);

  /** Copy the contents of the given record into this record. */
  void set(const NewRecord *other);

  /** Append this record with all attributes from the specified record. */
  void append(const NewRecord *other);

  /** Read a plaintext row into this record. Return the number of bytes read. */
  uint32_t read(const uint8_t *input);

  /** Read a plaintext row using streaming decryption */
  uint32_t read(StreamRowReader *reader);

  /** Read and decrypt an encrypted row into this record. Return the number of bytes read. */
  uint32_t read_encrypted(uint8_t *input);

  /** Write out this record in plaintext. Return the number of bytes written. */
  uint32_t write(uint8_t *output) const;

  /** Write out this record in plaintext using streaming encryption */
  uint32_t write(StreamRowWriter *writer) const;

  /** Encrypt and write out this record, returning the number of bytes written. */
  uint32_t write_encrypted(uint8_t *output);

  bool less_than(const NewRecord *other, int op_code) const;

  uint32_t get_key_prefix(int op_code) const;

  /**
   * Return the maximum number of bytes that could be written by write() for any row with the same
   * schema as this row.
   */
  uint32_t row_upper_bound() const;

  /**
   * Get a pointer to the attribute at the specified index (1-indexed). The pointer will begin at
   * the attribute type.
   */
  const uint8_t *get_attr(uint32_t attr_idx) const;

  uint8_t get_attr_type(uint32_t attr_idx) const;

  /** Get the length of the attribute at the specified index (1-indexed). */
  uint32_t get_attr_len(uint32_t attr_idx) const;

  /** Modify the length of the attribute at the specified index (1-indexed). */
  void set_attr_len(uint32_t attr_idx, uint32_t new_attr_len);

  /**
   * Get a pointer to the attribute at the specified index (1-indexed). The pointer will begin at
   * the attribute value.
   */
  const uint8_t *get_attr_value(uint32_t attr_idx) const;

  uint8_t *translate_attr_ptr(const NewRecord *other, const uint8_t *other_attr_ptr) const {
    return row + (other_attr_ptr - other->row);
  }

  /**
   * Set the value of the attribute at the specified index (1-indexed) to a new value of the same
   * length by copying the same number of bytes from new_attr_value as the existing attribute
   * occupies.
   */
  void set_attr_value(uint32_t attr_idx, const uint8_t *new_attr_value);

  /**
   * Append an attribute to the record by copying the attribute at the specified index (1-indexed)
   * from the other record.
   */
  void add_attr(const NewRecord *other, uint32_t attr_idx);

  /**
   * Append an attribute to the record by copying the attribute at the specified location.
   */
  void add_attr(const uint8_t *attr_ptr);

  /**
   * Append an attribute to the record.
   */
  void add_attr(uint8_t type, uint32_t len, const uint8_t *value);

  /**
   * Append an attribute to the record.
   */
  template<typename Type>
  void add_attr_val(Type value, bool dummy);

  /** Mark this record as a dummy by setting all its types to dummy types. */
  void mark_dummy();

  /** A row is a dummy if any of its types are dummy types. */
  bool is_dummy() const;

  void print() const;

  uint32_t num_cols() const {
    return *( (uint32_t *) row);
  }

private:
  void set_num_cols(uint32_t num_cols) {
    *reinterpret_cast<uint32_t *>(row) = num_cols;
  }

  uint8_t *row;
  uint32_t row_length;
};

/**
 * A record tagged with a table ID for use when joining a primary table with a foreign table.
 *
 * The table ID is stored as the first attribute of the row.
 *
 * This record type can optionally provide access to a join attribute, which is a specific attribute
 * from each primary and foreign row on which the join is performed. To access the join attribute,
 * first call init_join_attribute with an opcode specifying the position of the join attribute, then
 * use join_attr.
 */
class NewJoinRecord {
public:
  static const uint32_t primary_id = 0;
  static const uint32_t foreign_id = 1;

  static void init_dummy(NewRecord *dummy, int op_code);

  /**
   * Return the 1-indexed primary or foreign join attribute index associated with the given opcode,
   * if the opcode represents a one-column equijoin. If the opcode represents another kind of join,
   * return 0.
   */
  static uint32_t opcode_to_join_attr_idx(int op_code, bool is_primary);

  NewJoinRecord() : NewJoinRecord(ROW_UPPER_BOUND) {}

  NewJoinRecord(uint32_t upper_bound) : row(upper_bound), join_attr(NULL) {}

  /** Read a plaintext row into this record. Return the number of bytes read. */
  uint32_t read(uint8_t *input) {
    return row.read(input);
  }

  /** Read a plaintext row using streaming decryption */
  uint32_t read(StreamRowReader *reader) {
	return row.read(reader);
  }

  /** Read and decrypt an encrypted row into this record. Return the number of bytes read. */
  uint32_t read_encrypted(uint8_t *input) {
    return row.read_encrypted(input);
  }

  /** Write out the record in plaintext, returning the number of bytes written. */
  uint32_t write(uint8_t *output) {
    return row.write(output);
  }

  /** Write out the record in plaintext, returning the number of bytes written. */
  uint32_t write(StreamRowWriter *writer) {
    return row.write(writer);
  }

  /** Encrypt and write out the record, returning the number of bytes written. */
  uint32_t write_encrypted(uint8_t *output) {
    return row.write_encrypted(output);
  }

  /** Convert a standard record into a join record. */
  void set(bool is_primary, const NewRecord *record) {
    row.clear();
    uint32_t table_id = is_primary ? primary_id : foreign_id;
    row.add_attr(INT, 4, reinterpret_cast<const uint8_t *>(&table_id));
    row.append(record);
  }

  /** Copy the contents of other into this. */
  void set(NewJoinRecord *other) {
    row.set(&other->row);
    join_attr = row.translate_attr_ptr(&other->row, other->join_attr);
  }

  bool less_than(const NewJoinRecord *other, int op_code) const;

  uint32_t get_key_prefix(int op_code) const;

  /**
   * Return the maximum number of bytes that could be written by write() for any row with the same
   * schema as this row.
   */
  uint32_t row_upper_bound() const {
    return row.row_upper_bound();
  }

  /**
   * Given two join rows, concatenate their fields into merge.
   */
  void merge(const NewJoinRecord *other, NewRecord *merge) const;

  /** Read the join attribute from the row data into join_attr. */
  void init_join_attribute(int op_code);

  /** Return true if both records have the same join attribute. */
  bool join_attr_equals(const NewJoinRecord *other, int op_code) const;

  /**
   * Get a pointer to the attribute at the specified index (1-indexed). The pointer will begin at
   * the attribute type.
   */
  const uint8_t *get_attr(uint32_t attr_idx) const {
    return row.get_attr(attr_idx + 1);
  }

  /** Return true if the record belongs to the primary table based on its table ID. */
  bool is_primary() const {
    return *reinterpret_cast<const uint32_t *>(row.get_attr_value(1)) == primary_id;
  }

  /** Return true if the record contains all zeros, indicating a dummy record. */
  bool is_dummy() const {
    return row.num_cols() == 0;
  }

  /** Mark each attribute as a dummy attribute, used for column sort padding **/
  void mark_dummy();

  /**
   * Zero out the contents of this record. This causes sort-merge join to treat it as a dummy
   * record.
   */
  void reset_to_dummy() {
    row.clear();
  }

  uint32_t num_cols() const {
    return row.num_cols() - 1;
  }

  void print() const {
    printf("JoinRecord[row=");
    row.print();
    printf("]\n");
  }

  NewRecord get_row() {
    return row;
  }

private:
  NewRecord row;
  const uint8_t *join_attr; // pointer into row
};

template<typename RecordType>
class SortPointer {
  friend class RowWriter;
  friend class StreamRowWriter;
public:
  SortPointer() : rec(NULL), key_prefix(0) {}
  bool is_valid() const {
    return rec != NULL;
  }
  void init(RecordType *rec) {
    this->rec = rec;
  }
  void set(const SortPointer *other) {
    rec->set(other->rec);
    key_prefix = other->key_prefix;
  }
  void clear() {
    rec = NULL;
    key_prefix = 0;
  }
  uint32_t read(uint8_t *input, int op_code) {
    uint32_t result = rec->read(input);
    key_prefix = rec->get_key_prefix(op_code);
    return result;
  }

  uint32_t read(StreamRowReader *reader, int op_code) {
    uint32_t result = rec->read(reader);
    key_prefix = rec->get_key_prefix(op_code);
    return result;
  }
  
  bool less_than(const SortPointer *other, int op_code, uint32_t *num_deep_comparisons) const {
    if (key_prefix < other->key_prefix) {
      return true;
    } else if (key_prefix > other->key_prefix) {
      return false;
    } else {
      if (num_deep_comparisons != NULL) {
	(*num_deep_comparisons)++;
      }
      return rec->less_than(other->rec, op_code);
    }
  }

  void print() const {
    printf("SortPointer[key_prefix=%d, rec=", key_prefix);
    rec->print();
    printf("]\n");
  }
private:
  RecordType *rec;
  uint32_t key_prefix;
};

/**
 * Holds state for an ongoing group-by and aggregation operation. The column to group by is selected
 * by specifying GroupByType (a template instantiation of GroupBy), and the columns to aggregate on
 * are selected by specifying Agg1Type (a template instantation of Sum or Avg). Use Aggregator2 to
 * run two different aggregation functions on different columns at the same time.
 *
 * Supports aggregating a single record or another Aggregator1 (method aggregate), checking whether
 * other aggregators or records belong to the same group (method grouping_attrs_equal), serializing
 * and deserializing the state (methods read_encrypted and write_encrypted), and writing out the
 * result by appending columns to a NewRecord (method append_result).
 */
template<typename GroupByType, typename Agg1Type>
class Aggregator1 {
public:
  Aggregator1() : num_distinct(0), offset(0), g(), a1() {}

  void set(Aggregator1 *other) {
    this->num_distinct = other->num_distinct;
    this->offset = other->offset;
    this->g.set(&other->g);
    this->a1.set(&other->a1);
  }

  /**
   * Add the record to the aggregation state. If the record belongs to a different group, first
   * reset the aggregation state.
   */
  void aggregate(NewRecord *record) {
    GroupByType g2(record);
    if (g.equals(&g2)) {
      a1.add(record);
    } else {
      num_distinct++;
      g.set(&g2);
      a1.zero();
      a1.add(record);
    }
  }

  /**
   * Combine the aggregation state of the given aggregator with this one. Both aggregators must
   * belong to the same group.
   */
  void aggregate(Aggregator1 *other) {
    check(this->grouping_attrs_equal(other),
          "Attempted to combine partial aggregates with different grouping attributes\n");
    a1.add(&other->a1);
  }

  /**
   * Write the final aggregation result to the record by appending the grouping attribute and the
   * aggregation attribute. If dummy is true, mark the aggregation attribute as a dummy.
   */
  void append_result(NewRecord *record, bool dummy) {
    g.append_result(record);
    a1.append_result(record, dummy);
  }

  /** Read and decrypt a saved aggregation state. */
  uint32_t read_encrypted(uint8_t *input) {
    uint8_t *input_ptr = input;
    uint32_t agg_size = *reinterpret_cast<uint32_t *>(input_ptr); input_ptr += 4;
    check(agg_size == enc_size(AGG_UPPER_BOUND),
          "Aggregator length %d did not equal enc_size(AGG_UPPER_BOUND) = %d\n",
          agg_size, enc_size(AGG_UPPER_BOUND));
    uint8_t *tmp = (uint8_t *) malloc(AGG_UPPER_BOUND);
    decrypt(input_ptr, enc_size(AGG_UPPER_BOUND), tmp); input_ptr += enc_size(AGG_UPPER_BOUND);
    uint8_t *tmp_ptr = tmp;
    num_distinct = *reinterpret_cast<uint32_t *>(tmp_ptr); tmp_ptr += 4;
    offset = *reinterpret_cast<uint32_t *>(tmp_ptr); tmp_ptr += 4;
    g.read(tmp_ptr); tmp_ptr += ROW_UPPER_BOUND;
    tmp_ptr += a1.read_partial_result(tmp_ptr);
    free(tmp);
    return input_ptr - input;
  }

  /** Encrypt and write out the current aggregation state. */
  uint32_t write_encrypted(uint8_t *output) {
    uint8_t *tmp = (uint8_t *) malloc(AGG_UPPER_BOUND);
    uint8_t *tmp_ptr = tmp;
    *reinterpret_cast<uint32_t *>(tmp_ptr) = num_distinct; tmp_ptr += 4;
    *reinterpret_cast<uint32_t *>(tmp_ptr) = offset; tmp_ptr += 4;
    g.write_whole_row(tmp_ptr); tmp_ptr += ROW_UPPER_BOUND;
    tmp_ptr += a1.write_partial_result(tmp_ptr);

    uint8_t *output_ptr = output;
    *reinterpret_cast<uint32_t *>(output_ptr) = enc_size(AGG_UPPER_BOUND); output_ptr += 4;
    encrypt(tmp, AGG_UPPER_BOUND, output_ptr); output_ptr += enc_size(AGG_UPPER_BOUND);
    free(tmp);
    return output_ptr - output;
  }

  /** Get the number of groups this aggregator has seen so far. */
  uint32_t get_num_distinct() {
    return num_distinct;
  }

  void set_num_distinct(uint32_t num_distinct) {
    this->num_distinct = num_distinct;
  }

  void set_offset(uint32_t offset) {
    this->offset = offset;
  }

  /** Return true if the given aggregator belongs to the same group as this one. */
  bool grouping_attrs_equal(Aggregator1 *other) {
    return g.equals(&other->g);
  }

  /**
   * Return true if the given record belongs to the same group as this one. A dummy record is
   * treated as belonging to no group.
   */
  bool grouping_attrs_equal(NewRecord *record) {
    if (record->is_dummy()) {
      return false;
    } else {
      GroupByType g2(record);
      return g.equals(&g2);
    }
  }

  void print() {
    printf("Aggregator1[num_distinct=%d, offset=%d, g=");
    g.print();
    printf(", a1=");
    a1.print();
    printf("]\n");
  }

private:
  uint32_t num_distinct;
  uint32_t offset;
  GroupByType g;
  Agg1Type a1;
};

/** Holds state for an ongoing group-by and aggregation operation. See Aggregator1. */
template<typename GroupByType, typename Agg1Type, typename Agg2Type>
class Aggregator2 {
public:
  Aggregator2() : num_distinct(0), offset(0), g(), a1(), a2() {}

  void set(Aggregator2 *other) {
    this->num_distinct = other->num_distinct;
    this->offset = other->offset;
    this->g.set(&other->g);
    this->a1.set(&other->a1);
    this->a2.set(&other->a2);
  }

  void aggregate(NewRecord *record) {
    GroupByType g2(record);
    if (g.equals(&g2)) {
      a1.add(record);
      a2.add(record);
    } else {
      num_distinct++;
      g.set(&g2);
      a1.zero();
      a1.add(record);
      a2.zero();
      a2.add(record);
    }
  }

  void aggregate(Aggregator2 *other) {
    check(this->grouping_attrs_equal(other),
          "Attempted to combine partial aggregates with different grouping attributes\n");
    a1.add(&other->a1);
    a2.add(&other->a2);
  }

  /**
   * Write the final aggregation result to the record by appending the grouping attribute and both
   * aggregation attributes. If dummy is true, mark the aggregation attributes as dummies.
   */
  void append_result(NewRecord *record, bool dummy) {
    g.append_result(record);
    a1.append_result(record, dummy);
    a2.append_result(record, dummy);
  }

  uint32_t read_encrypted(uint8_t *input) {
    uint8_t *input_ptr = input;
    uint32_t agg_size = *reinterpret_cast<uint32_t *>(input_ptr); input_ptr += 4;
    check(agg_size == enc_size(AGG_UPPER_BOUND),
          "Aggregator length %d did not equal enc_size(AGG_UPPER_BOUND) = %d\n",
          agg_size, enc_size(AGG_UPPER_BOUND));
    uint8_t *tmp = (uint8_t *) malloc(AGG_UPPER_BOUND);
    decrypt(input_ptr, enc_size(AGG_UPPER_BOUND), tmp); input_ptr += enc_size(AGG_UPPER_BOUND);
    uint8_t *tmp_ptr = tmp;
    num_distinct = *reinterpret_cast<uint32_t *>(tmp_ptr); tmp_ptr += 4;
    offset = *reinterpret_cast<uint32_t *>(tmp_ptr); tmp_ptr += 4;
    g.read(tmp_ptr); tmp_ptr += ROW_UPPER_BOUND;
    tmp_ptr += a1.read_partial_result(tmp_ptr);
    tmp_ptr += a2.read_partial_result(tmp_ptr);
    free(tmp);
    return input_ptr - input;
  }

  uint32_t write_encrypted(uint8_t *output) {
    uint8_t *tmp = (uint8_t *) malloc(AGG_UPPER_BOUND);
    uint8_t *tmp_ptr = tmp;
    *reinterpret_cast<uint32_t *>(tmp_ptr) = num_distinct; tmp_ptr += 4;
    *reinterpret_cast<uint32_t *>(tmp_ptr) = offset; tmp_ptr += 4;
    g.write_whole_row(tmp_ptr); tmp_ptr += ROW_UPPER_BOUND;
    tmp_ptr += a1.write_partial_result(tmp_ptr);
    tmp_ptr += a2.write_partial_result(tmp_ptr);

    uint8_t *output_ptr = output;
    *reinterpret_cast<uint32_t *>(output_ptr) = enc_size(AGG_UPPER_BOUND); output_ptr += 4;
    encrypt(tmp, AGG_UPPER_BOUND, output_ptr); output_ptr += enc_size(AGG_UPPER_BOUND);
    free(tmp);
    return output_ptr - output;
  }

  uint32_t get_num_distinct() {
    return num_distinct;
  }

  void set_num_distinct(uint32_t num_distinct) {
    this->num_distinct = num_distinct;
  }

  void set_offset(uint32_t offset) {
    this->offset = offset;
  }

  bool grouping_attrs_equal(Aggregator2 *other) {
    return g.equals(&other->g);
  }

  bool grouping_attrs_equal(NewRecord *record) {
    if (record->is_dummy()) {
      return false;
    } else {
      GroupByType g2(record);
      return g.equals(&g2);
    }
  }

  void print() {
    printf("Aggregator2[num_distinct=%d, offset=%d, g=");
    g.print();
    printf(", a1=");
    a1.print();
    printf(", a2=");
    a2.print();
    printf("]\n");
  }

private:
  uint32_t num_distinct;
  uint32_t offset;
  GroupByType g;
  Agg1Type a1;
  Agg2Type a2;
};

/**
 * Holds state for an ongoing group-by operation. The column to group by is selected by specifying
 * Column (1-indexed). Supports reading the grouping column from a record (constructor),
 */
template<uint32_t Column>
class GroupBy {
public:
  GroupBy() : row(), attr(NULL) {}

  GroupBy(NewRecord *record) : row() {
    row.set(record);
    if (row.num_cols() != 0) {
      this->attr = row.get_attr(Column);
    } else {
      this->attr = NULL;
    }
  }

  /** Update this GroupBy object to track a different group. */
  void set(GroupBy *other) {
    row.set(&other->row);
    if (row.num_cols() != 0) {
      this->attr = row.get_attr(Column);
    } else {
      this->attr = NULL;
    }
  }

  /**
   * Read an entire plaintext row and extract the grouping columns. Return the number of bytes in
   * the row. If the row is empty (has 0 columns), then this GroupBy object will not track any
   * group.
   */
  uint32_t read(uint8_t *input) {
    uint32_t result = row.read(input);
    if (row.num_cols() != 0) {
      this->attr = row.get_attr(Column);
    } else {
      this->attr = NULL;
    }
    return result;
  }

  /** Return true if both GroupBy objects are tracking the same group. */
  bool equals(GroupBy *other) {
    if (this->attr != NULL && other->attr != NULL) {
      return attrs_equal(this->attr, other->attr);
    } else {
      return false;
    }
  }

  /** Write the grouping attribute by appending it to the given record. */
  void append_result(NewRecord *rec) const {
    rec->add_attr(attr);
  }

  /** Write an entire row containing the grouping column to output and return num bytes written. */
  uint32_t write_whole_row(uint8_t *output) {
    return row.write(output);
  }

  void print() {
    printf("GroupBy[Column=%d, row=", Column);
    row.print();
    printf("]\n");
  }

private:
  NewRecord row;
  const uint8_t *attr; // pointer into row
};

/**
 * Holds state for an ongoing group-by operation. The columns to group by are selected by specifying
 * Column1 and Column2 (1-indexed). Supports reading the grouping column from a record
 * (constructor),
 */
template<uint32_t Column1, uint32_t Column2>
class GroupBy2 {
public:
  GroupBy2() : row(), attr1(NULL), attr2(NULL) {}

  GroupBy2(NewRecord *record) : row() {
    row.set(record);
    if (row.num_cols() != 0) {
      this->attr1 = row.get_attr(Column1);
      this->attr2 = row.get_attr(Column2);
    } else {
      this->attr1 = NULL;
      this->attr2 = NULL;
    }
  }

  /** Update this GroupBy object to track a different group. */
  void set(GroupBy2 *other) {
    row.set(&other->row);
    if (row.num_cols() != 0) {
      this->attr1 = row.get_attr(Column1);
      this->attr2 = row.get_attr(Column2);
    } else {
      this->attr1 = NULL;
      this->attr2 = NULL;
    }
  }

  /**
   * Read an entire plaintext row and extract the grouping columns. Return the number of bytes in
   * the row. If the row is empty (has 0 columns), then this GroupBy object will not track any
   * group.
   */
  uint32_t read(uint8_t *input) {
    uint32_t result = row.read(input);
    if (row.num_cols() != 0) {
      this->attr1 = row.get_attr(Column1);
      this->attr2 = row.get_attr(Column2);
    } else {
      this->attr1 = NULL;
      this->attr2 = NULL;
    }
    return result;
  }

  /** Return true if both GroupBy objects are tracking the same group. */
  bool equals(GroupBy2 *other) {
    if (this->attr1 != NULL && other->attr1 != NULL
        && this->attr2 != NULL && other->attr2 != NULL) {
      return attrs_equal(this->attr1, other->attr1) && attrs_equal(this->attr2, other->attr2);
    } else {
      return false;
    }
  }

  /** Write the grouping attributes by appending them to the given record. */
  void append_result(NewRecord *rec) const {
    rec->add_attr(attr1);
    rec->add_attr(attr2);
  }

  /** Write an entire row containing the grouping column to output and return num bytes written. */
  uint32_t write_whole_row(uint8_t *output) {
    return row.write(output);
  }

  void print() {
    printf("GroupBy2[Column1=%d, Column2=%d, row=", Column1, Column2);
    row.print();
    printf("]\n");
  }

private:
  NewRecord row;
  const uint8_t *attr1; // pointer into row
  const uint8_t *attr2; // pointer into row
};

/**
 * Holds state for an ongoing sum aggregation operation. The column to sum is selected by specifying
 * Column (1-indexed) and the type of that column is specified using Type. Supports resetting and
 * aggregating (methods zero and add), reading/writing partial aggregation state (methods
 * read_partial_result and write_partial_result), and writing the final aggregation result (method
 * append_result).
 */
template<uint32_t Column, typename InType, typename OutType>
class Sum {
public:
  Sum() : sum() {}

  /** Update the sum to an arbitrary value. */
  void set(Sum *other) {
    this->sum = other->sum;
  }

  /** Reset the sum to zero. */
  void zero() {
    sum = OutType();
  }

  /** Add in the value from a single record. */
  void add(NewRecord *record) {
    sum += *reinterpret_cast<const InType *>(record->get_attr_value(Column));
  }

  /** Combine the value from another Sum object. */
  void add(Sum *other) {
    sum += other->sum;
  }

  /** Read a partial sum (one plaintext attribute) and return the number of bytes read. */
  uint32_t read_partial_result(uint8_t *input) {
    return read_attr<OutType>(input, reinterpret_cast<uint8_t *>(&sum));
  }

  /** Write the partial sum as a single plaintext attribute and return num bytes written. */
  uint32_t write_partial_result(uint8_t *output) {
    return write_attr<OutType>(output, sum, false);
  }

  /** Write the final sum by appending it to the given record. */
  void append_result(NewRecord *rec, bool dummy) const {
    rec->add_attr_val<OutType>(sum, dummy);
  }

  void print() {
    printf("Sum[sum=%f]\n", static_cast<float>(sum));
  }

private:
  OutType sum;
};

/**
 * Holds state for an ongoing average (mean) aggregation operation. See Sum.
 */
template<uint32_t Column, typename InType, typename OutType>
class Avg {
public:
  Avg() : sum(), count(0) {}

  void set(Avg *other) {
    this->sum = other->sum;
    this->count = other->count;
  }

  void zero() {
    sum = OutType();
    count = 0;
  }

  void add(NewRecord *record) {
    sum += *reinterpret_cast<const InType *>(record->get_attr_value(Column));
    count++;
  }

  void add(Avg *other) {
    sum += other->sum;
    count += other->count;
  }

  /** Read a partial average (two plaintext attributes: sum and count) and return num bytes read. */
  uint32_t read_partial_result(uint8_t *input) {
    uint8_t *input_ptr = input;
    input_ptr += read_attr<OutType>(input_ptr, reinterpret_cast<uint8_t *>(&sum));
    input_ptr += read_attr<uint64_t>(input_ptr, reinterpret_cast<uint8_t *>(&count));
    return input_ptr - input;
  }

  /** Write the partial average (two plaintext attrs: sum and count); return num bytes written. */
  uint32_t write_partial_result(uint8_t *output) {
    uint8_t *output_ptr = output;
    output_ptr += write_attr<OutType>(output_ptr, sum, false);
    output_ptr += write_attr<uint64_t>(output_ptr, count, false);
    return output_ptr - output;
  }

  /** Write the final average by appending it to the given record. */
  void append_result(NewRecord *rec, bool dummy) const {
    OutType avg = static_cast<OutType>(static_cast<double>(sum) / static_cast<double>(count));
    rec->add_attr_val<OutType>(avg, dummy);
  }

  void print() {
    printf("Avg[sum=%f, count=%d]\n", sum, count);
  }

private:
  OutType sum;
  uint64_t count;
};

/**
 * Holds state for an ongoing min aggregation operation. See Sum.
 */
template<uint32_t Column, typename InType, typename OutType>
class Min {
public:
  Min() : min() {}

  void set(Min *other) {
    this->min = other->min;
  }

  void zero() {
    min = std::numeric_limits<OutType>::max();
  }

  void add(NewRecord *record) {
    InType val = *reinterpret_cast<const InType *>(record->get_attr_value(Column));
    if (val < min) {
      min = val;
    }
  }

  void add(Min *other) {
    if (other->min < min) {
      min = other->min;
    }
  }

  /** Read a partial min and return num bytes read. */
  uint32_t read_partial_result(uint8_t *input) {
    uint8_t *input_ptr = input;
    input_ptr += read_attr<OutType>(input_ptr, reinterpret_cast<uint8_t *>(&min));
    return input_ptr - input;
  }

  /** Write the partial min and return num bytes written. */
  uint32_t write_partial_result(uint8_t *output) {
    uint8_t *output_ptr = output;
    output_ptr += write_attr<OutType>(output_ptr, min, false);
    return output_ptr - output;
  }

  /** Write the final min by appending it to the given record. */
  void append_result(NewRecord *rec, bool dummy) const {
    rec->add_attr_val<OutType>(min, dummy);
  }

  void print() {
    printf("Min[min=%f]\n", min);
  }

private:
  OutType min;
};

/**
 * Manages reading multiple encrypted rows from a buffer.
 *
 * To read rows, initialize an empty row object and repeatedly call the appropriate read function
 * with it, which will populate the row object with the next row.
 */
class RowReader {
public:
  RowReader(uint8_t *buf, uint8_t *buf_end, Verify *verify_set)
    : buf(buf), buf_end(buf_end), block_pos(NULL), block_num_rows(0), block_rows_read(0),
      verify_set(verify_set) {
    block_start = (uint8_t *) malloc(MAX_BLOCK_SIZE);
    read_encrypted_block();
  }

  RowReader(uint8_t *buf, uint8_t *buf_end) : RowReader(buf, buf_end, NULL) { }

  ~RowReader() {
    free(block_start);
  }

  void read(NewRecord *row) {
    maybe_advance_block();
    block_pos += row->read(block_pos);
    block_rows_read++;
  }
  void read(NewJoinRecord *row) {
    maybe_advance_block();
    block_pos += row->read(block_pos);
    block_rows_read++;
  }

  template<typename RecordType>
  void read(SortPointer<RecordType> *ptr, int op_code) {
    maybe_advance_block();
    block_pos += ptr->read(block_pos, op_code);
    block_rows_read++;
  }

  bool has_next() const {
    return block_has_next() || buf_has_next();
  }

  void close_and_verify(int op_code, uint32_t num_part, int index) {
    (void)op_code;
    (void)num_part;
    (void)index;

  }

private:
  void add_parent(uint32_t task_id) {
    // simply add the task ID to the verify set
    if (verify_set != NULL) {
      verify_set->add_node(task_id);
    }
  }

  /** Return true if rows remain in the current block. */
  bool block_has_next() const {
    return block_rows_read < block_num_rows;
  }

  /** Return true if blocks beyond the current one remain in the buffer. */
  bool buf_has_next() const {
    return buf < buf_end;
  }

  void read_encrypted_block() {
    while (!block_has_next() && buf_has_next()) {
      uint32_t block_enc_size = *reinterpret_cast<uint32_t *>(buf); buf += 4;
      block_num_rows = *reinterpret_cast<uint32_t *>(buf); buf += 4;
      buf += 4; // row_upper_bound
      uint32_t task_id = *reinterpret_cast<uint32_t *>(buf); buf += 4;

      block_pos = block_start;
      block_rows_read = 0;

      add_parent(task_id);
      if (verify_set != NULL) {
        verify_set->mac(buf + SGX_AESGCM_IV_SIZE);
      }
      decrypt_with_aad(buf, block_enc_size, block_start, buf - 16, 16);

      buf += block_enc_size;
    }
  }

  void maybe_advance_block() {
    if (!block_has_next()) {
      read_encrypted_block();
    }
  }

  uint8_t *buf;
  uint8_t * const buf_end;

  uint8_t *block_start;
  uint8_t *block_pos;
  uint32_t block_num_rows;
  uint32_t block_rows_read;

  Verify *verify_set;
};

/**
 * Manages reading multiple encrypted rows from a buffer.
 *
 * To read rows, initialize an empty row object and repeatedly call the appropriate read function
 * with it, which will populate the row object with the next row.
 */
class FlatbuffersRowReader {
public:
  FlatbuffersRowReader(uint8_t *buf, size_t len)
    : rows_read(0) {
    flatbuffers::Verifier v1(buf, len);
    check(tuix::VerifyEncryptedBlockBuffer(v1),
          "Corrupt EncryptedBlock %p of length %d\n", buf, len);
    auto encrypted_block = tuix::GetEncryptedBlock(buf);
    num_rows = encrypted_block->num_rows();

    const size_t rows_len = dec_size(encrypted_block->enc_rows()->size());
    rows_buf.reset(new uint8_t[rows_len]);
    decrypt(encrypted_block->enc_rows()->data(), encrypted_block->enc_rows()->size(),
            rows_buf.get());
    printf("Decrypted %d rows, plaintext is %d bytes\n", num_rows, rows_len);
    flatbuffers::Verifier v2(rows_buf.get(), rows_len);
    check(tuix::VerifyRowsBuffer(v2),
          "Corrupt Rows %p of length %d\n", rows_buf.get(), rows_len);

    rows = tuix::GetRows(rows_buf.get());
    check(rows->rows()->size() == num_rows,
          "EncryptedBlock claimed to contain %d rows but actually contains %d rows\n",
          num_rows == rows->rows()->size());
  }

  const tuix::Row *next() {
    return rows->rows()->Get(rows_read++);
  }

private:
  std::unique_ptr<uint8_t> rows_buf;
  const tuix::Rows *rows;
  uint32_t rows_read;
  uint32_t num_rows;
};

class UntrustedMemoryAllocator : public flatbuffers::simple_allocator {
public:
  virtual uint8_t *allocate(size_t size) const {
    uint8_t *result = nullptr;
    ocall_malloc(size, &result);
    return result;
  }
  virtual void deallocate(uint8_t *p) const {
    ocall_free(p);
  }
};

class FlatbuffersRowWriter {
public:
  FlatbuffersRowWriter()
    : builder(), rows_vector(), untrusted_alloc() {
  }

  void write(const tuix::Row *row) {
    rows_vector.push_back(flatbuffers_copy(row, builder));
  }

  void close() {
    tuix::FinishRowsBuffer(builder, tuix::CreateRowsDirect(builder, &rows_vector));
    size_t enc_rows_len = enc_size(builder.GetSize());
    uint8_t *enc_rows = nullptr;
    ocall_malloc(enc_rows_len, &enc_rows);
    encrypt(builder.GetBufferPointer(), builder.GetSize(), enc_rows);

    enc_block_builder.reset(
      new flatbuffers::FlatBufferBuilder(enc_rows_len * 2, &untrusted_alloc));
    tuix::FinishEncryptedBlockBuffer(
      *enc_block_builder,
      tuix::CreateEncryptedBlock(
        *enc_block_builder,
        rows_vector.size(),
        enc_block_builder->CreateVector(enc_rows, enc_rows_len)));

    ocall_free(enc_rows);
  }

  uint8_t *output_buffer() {
    uint8_t *buf = nullptr;
    ocall_malloc(output_size(), &buf);
    memcpy(buf, enc_block_builder->GetBufferPointer(), output_size());
    return buf;
  }

  size_t output_size() {
    return enc_block_builder->GetSize();
  }

  uint32_t output_num_rows() {
    return rows_vector.size();
  }

private:
  flatbuffers::FlatBufferBuilder builder;
  std::vector<flatbuffers::Offset<tuix::Row>> rows_vector;

  // For writing the resulting EncryptedBlock
  UntrustedMemoryAllocator untrusted_alloc;
  std::unique_ptr<flatbuffers::FlatBufferBuilder> enc_block_builder;
};

void print(const tuix::Row *in);

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
      return flatbuffers_copy<tuix::Field>(
        static_cast<const tuix::Literal *>(expr->expr())->value(), builder);
    case tuix::ExprUnion_GreaterThan:
    {
      auto gt = static_cast<const tuix::GreaterThan *>(expr->expr());
      // Note: These temporary pointers will be invalidated when we next write to builder
      const tuix::Field *left =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, gt->left()));
      const tuix::Field *right =
        flatbuffers::GetTemporaryPointer(builder, eval_helper(row, gt->right()));
      check(left->value_type() == right->value_type(),
            "tuix::GreaterThan can't compare values of different types (%s and %s)\n",
            tuix::EnumNameFieldUnion(left->value_type()),
            tuix::EnumNameFieldUnion(right->value_type()));
      bool result_is_null = left->is_null() || right->is_null();
      bool result = false;
      if (!result_is_null) {
        switch (left->value_type()) {
        case tuix::FieldUnion_IntegerField:
          result = static_cast<const tuix::IntegerField *>(left->value())->value()
            > static_cast<const tuix::IntegerField *>(right->value())->value();
          break;
        case tuix::FieldUnion_LongField:
          result = static_cast<const tuix::LongField *>(left->value())->value()
            > static_cast<const tuix::LongField *>(right->value())->value();
          break;
        case tuix::FieldUnion_FloatField:
          result = static_cast<const tuix::FloatField *>(left->value())->value()
            > static_cast<const tuix::FloatField *>(right->value())->value();
          break;
        case tuix::FieldUnion_DoubleField:
          result = static_cast<const tuix::DoubleField *>(left->value())->value()
            > static_cast<const tuix::DoubleField *>(right->value())->value();
          break;
        default:
          printf("Can't evaluate tuix::GreaterThan on %s\n",
                 tuix::EnumNameFieldUnion(left->value_type()));
          assert(false);
        }
      }
      // Writing the result invalidates the left and right temporary pointers
      return tuix::CreateField(
        builder,
        tuix::FieldUnion_BooleanField,
        tuix::CreateBooleanField(builder, result).Union(),
        result_is_null);
    }
    default:
      printf("Can't evaluate expression of type %s\n",
             tuix::EnumNameExprUnion(expr->expr_type()));
      assert(false);
      return flatbuffers::Offset<tuix::Field>();
    }
  }

  flatbuffers::FlatBufferBuilder builder;
  const tuix::Expr *expr;
};

class IndividualRowReaderV {
 public:
  IndividualRowReaderV(uint8_t *buf_input, Verify *verify_set) {
    // read the task ID
    buf = buf_input;
    buf_start = buf_input;
    this->verify_set = verify_set;

  }

 IndividualRowReaderV(uint8_t *buf) : IndividualRowReaderV(buf, NULL) {}

  void read(NewRecord *row) {
    read_tag();
    buf += row->read_encrypted(buf);
  }
  void read(NewJoinRecord *row) {
    read_tag();
    buf += row->read_encrypted(buf);
  }
  template<typename AggregatorType>
    void read(AggregatorType *agg) {
    read_tag();
    buf += agg->read_encrypted(buf);
  }

  void read_tag() {
    if (verify_set != NULL) {
      uint32_t self_task_id = *reinterpret_cast<uint32_t *>(buf);
      buf += 4;
      verify_set->add_node(self_task_id);
    }
  }

 private:
  uint8_t *buf;
  uint8_t *buf_start;
  Verify *verify_set;
};

class IndividualRowReader {
public:
 IndividualRowReader(uint8_t *buf_input) { buf = buf_input; }

  void read(NewRecord *row) {
    buf += row->read_encrypted(buf);
  }
  void read(NewJoinRecord *row) {
    buf += row->read_encrypted(buf);
  }
  template<typename AggregatorType>
  void read(AggregatorType *agg) {
    buf += agg->read_encrypted(buf);
  }

private:
  uint8_t *buf;
};


/**
 * Manages encrypting and writing out multiple rows to an output buffer. Either all rows must share
 * the same schema, or a row upper bound must be passed to the constructor.
 *
 * After writing all rows, make sure to call close().
 */
class RowWriter {
public:
  RowWriter(uint8_t *buf, uint32_t row_upper_bound)
    : buf_start(buf), buf_pos(buf), row_upper_bound(row_upper_bound), block_num_rows(0),
      block_padded_len(0) {
    self_task_id = 0;
    block_start = (uint8_t *) malloc(MAX_BLOCK_SIZE);
    block_pos = block_start;

    // just use a fake IV
    mac_obj = new MAC;
  }

  RowWriter(uint8_t *buf) : RowWriter(buf, 0) { }

  ~RowWriter() {
    free(block_start);
    delete mac_obj;
  }

  void set_self_task_id(uint32_t self_task_id) {
    this->self_task_id = self_task_id;
  }

  void write(NewRecord *row) {
    maybe_finish_block(ROW_UPPER_BOUND);
    uint32_t delta = row->write(block_pos);
    check(delta <= ((row_upper_bound == 0) ? ROW_UPPER_BOUND : row_upper_bound),
          "Wrote %d, which is more than row_upper_bound = %d\n",
          delta, row_upper_bound == 0 ? ROW_UPPER_BOUND : row_upper_bound);
    block_pos += delta;
    block_num_rows++;
    if (row_upper_bound == 0) {
      row_upper_bound = row->row_upper_bound();
    }
    block_padded_len += row_upper_bound;
  }

  void write(NewJoinRecord *row) {
    maybe_finish_block(ROW_UPPER_BOUND);
    uint32_t delta = row->write(block_pos);
    check(delta <= ((row_upper_bound == 0) ? ROW_UPPER_BOUND : row_upper_bound),
          "Wrote %d, which is more than row_upper_bound = %d\n",
          delta, row_upper_bound == 0 ? ROW_UPPER_BOUND : row_upper_bound);
    block_pos += delta;
    block_num_rows++;
    if (row_upper_bound == 0) {
      row_upper_bound = row->row_upper_bound();
    }
    block_padded_len += row_upper_bound;
  }

  template<typename RecordType>
  void write(SortPointer<RecordType> *ptr) {
    write(ptr->rec);
  }

  void finish_block() {
    *reinterpret_cast<uint32_t *>(buf_pos) = enc_size(block_padded_len); buf_pos += 4;
    *reinterpret_cast<uint32_t *>(buf_pos) = block_num_rows; buf_pos += 4;
    *reinterpret_cast<uint32_t *>(buf_pos) = row_upper_bound; buf_pos += 4;
    *reinterpret_cast<uint32_t *>(buf_pos) = self_task_id; buf_pos += 4;

    encrypt_with_aad(block_start, block_padded_len, buf_pos, buf_pos - 16, 16);
    mac_obj->mac(buf_pos + SGX_AESGCM_IV_SIZE, SGX_AESGCM_MAC_SIZE);

    buf_pos += enc_size(block_padded_len);

    block_pos = block_start;
    block_num_rows = 0;
    block_padded_len = 0;
  }

  void close() {
    finish_block();
  }

  uint32_t bytes_written() {
    return buf_pos - buf_start;
  }

private:
  void maybe_finish_block(uint32_t next_row_size) {
    if (block_padded_len + next_row_size > MAX_BLOCK_SIZE) {
      finish_block();
    }
  }

  uint8_t * const buf_start;
  uint8_t *buf_pos;
  uint32_t row_upper_bound;

  uint8_t *block_start;
  uint8_t *block_pos;
  uint32_t block_num_rows;
  uint32_t block_padded_len;

  uint32_t self_task_id;

  MAC *mac_obj;
};

class IndividualRowWriterV {
public:
 IndividualRowWriterV(uint8_t *buf) : buf_start(buf), buf(buf) {
    this->buf = buf_start;
    self_task_id = 0;
  }

  void write(NewRecord *row) {
    write_tag();
    uint32_t delta = row->write_encrypted(buf);
    check(delta <= enc_size(ROW_UPPER_BOUND),
          "Wrote %d, which is more than enc_size(ROW_UPPER_BOUND)\n", delta);
    buf += delta;
  }

  void write(NewJoinRecord *row) {
    write_tag();
    buf += row->write_encrypted(buf);
  }

  template<typename AggregatorType>
  void write(AggregatorType *agg) {
    write_tag();
    buf += agg->write_encrypted(buf);
  }

  void set_self_task_id(uint32_t self_task_id) {
    this->self_task_id = self_task_id;
  }

  void write_tag() {
    *reinterpret_cast<uint32_t *>(buf) = self_task_id;
    buf += 4;
  }

  void close() {
  }

  uint32_t bytes_written() {
    return buf - buf_start;
  }

private:
  uint8_t * const buf_start;
  uint8_t *buf;
  uint32_t self_task_id;
};


class IndividualRowWriter {
public:
 IndividualRowWriter(uint8_t *buf) : buf_start(buf), buf(buf) { }

  void write(NewRecord *row) {
    uint32_t delta = row->write_encrypted(buf);
    check(delta <= enc_size(ROW_UPPER_BOUND),
          "Wrote %d, which is more than enc_size(ROW_UPPER_BOUND)\n", delta);
    buf += delta;
  }

  void write(NewJoinRecord *row) {
    buf += row->write_encrypted(buf);
  }

  template<typename AggregatorType>
  void write(AggregatorType *agg) {
    buf += agg->write_encrypted(buf);
  }

  void close() {
  }

  uint32_t bytes_written() {
    return buf - buf_start;
  }

private:
  uint8_t * const buf_start;
  uint8_t *buf;
};



/**
 * Manages reading multiple stream-encrypted rows from a buffer. Rows are organized into blocks of
 * up to MAX_BLOCK_SIZE bytes.
 *
 * To read rows, initialize an empty row object and repeatedly call the appropriate read function
 * with it, which will populate the row object with the next row.
 *
 * This class performs no bounds checking; the caller is responsible for knowing how many rows the
 * buffer contains.
 */
class StreamRowReader {
 public:
  StreamRowReader(uint8_t *buf, uint8_t *buf_end)
    : cipher(NULL), buf(buf), buf_end(buf_end), cur_block_num(0) {
    verify_set = new std::set<uint32_t>();
    this->read_encrypted_block();
  }

  StreamRowReader(uint8_t *buf) : StreamRowReader(buf, NULL) { }

  ~StreamRowReader() {
    delete cipher;
    delete verify_set;
  }

  void read(NewRecord *row) {
    maybe_advance_block();
    block_pos += row->read(this);
    ++block_rows_read;
  }

  void read(NewJoinRecord *row) {
    maybe_advance_block();
    block_pos += row->read(this);
    ++block_rows_read;
  }

  template<typename RecordType>
  void read(SortPointer<RecordType> *ptr, int op_code) {
    maybe_advance_block();
    block_pos += ptr->read(this, op_code);
    block_rows_read++;
  }

  void read_bytes(uint8_t *output, uint32_t num_bytes) {
    cipher->decrypt(output, num_bytes);
  }

  bool has_next() const {
    bool rows_remain_in_block = block_rows_read < block_num_rows;
    bool blocks_remain_in_buf = buf < buf_end;
    return rows_remain_in_block || blocks_remain_in_buf;
  }


  void close_and_verify(int op_code, uint32_t num_part, int index) {
    (void) op_code;
    (void) num_part;
    (void) index;
  }

 private:
  
  void add_parent(uint32_t task_id) {
    // simply add the task ID to the verify set
    verify_set->insert(task_id);
  }
  
  void read_encrypted_block() {
    uint32_t block_enc_size = *reinterpret_cast<uint32_t *>(buf); buf += 4;
    block_num_rows = *reinterpret_cast<uint32_t *>(buf); buf += 4;
    buf += 4; // row_upper_bound

    uint32_t task_id = *reinterpret_cast<uint32_t *>(buf); buf += 4;
    add_parent(task_id);
   

    if (cipher == NULL) {
      cipher = new StreamDecipher(buf, block_enc_size);
    } else {
      cipher->reset(buf, block_enc_size);
    }
    
    buf += block_enc_size;
    block_start = buf;
    block_pos = block_start;
    block_rows_read = 0;
    ++cur_block_num;
  }

  void maybe_advance_block() {
    if (block_rows_read >= block_num_rows) {
      read_encrypted_block();
    }
  }


  StreamDecipher *cipher;
  uint8_t *buf;
  uint8_t *buf_end;
  uint8_t *block_start;
  uint8_t *block_pos;
  uint32_t block_num_rows;
  uint32_t block_rows_read;
  uint32_t cur_block_num;
  std::set<uint32_t> *verify_set;
};


/**
 * Manages writing out encrypted rows to a single encrypted buffer; supports streaming
 */
class StreamRowWriter {
 public:
  StreamRowWriter(uint8_t *buf) {
    buf_start = buf;
    buf_pos = buf;
    cipher = new StreamCipher(buf_start + 12);
    block_len = 0;
    block_num_rows = 0;
    opcode = 0;
    part = 0;
  }

  ~StreamRowWriter() {
	delete cipher;
  }

  void set_opcode(uint32_t opcode) {
    this->opcode = opcode;
  }

  void set_part_index(uint32_t part) {
    this->part = part;
  }

  uint32_t write(NewRecord *row) {
	maybe_finish_block();
	uint32_t len = row->write(this);
	++block_num_rows;

	return len;
  }

  uint32_t write(NewJoinRecord *row) {
	maybe_finish_block();
	uint32_t len = row->write(this);
	++block_num_rows;

	return len;
  }

  template<typename RecordType>
  void write(SortPointer<RecordType> *ptr) {
    write(ptr->rec);
  }

  void write_bytes(uint8_t *input, uint32_t size) {
	cipher->encrypt(input, size);
  }
  
  void finish() {
	finish_block();
  }

  void close() {
	finish_block();
  }

  uint32_t bytes_written() {
	return buf_pos - buf_start;
  }

 private:

  void finish_block() {
    cipher->finish();
    uint32_t w_bytes = cipher->bytes_written();

    *reinterpret_cast<uint32_t *>(buf_pos) = w_bytes;
    buf_pos += 4;
    *reinterpret_cast<uint32_t *>(buf_pos) = block_num_rows;
    buf_pos += 4;
    *reinterpret_cast<uint32_t *>(buf_pos) = ROW_UPPER_BOUND;
    buf_pos += 4;

    uint32_t task_id = task_id_parser(opcode, part);
    *reinterpret_cast<uint32_t *>(buf_pos) = task_id; buf_pos += 4;

    debug("[StreamRowWriter::finish_block] w_bytes is %u, block_num_rows is %u\n", w_bytes, block_num_rows);

    block_num_rows = 0;
    block_len = 0;
    buf_pos += w_bytes;
	
    cipher->reset(buf_pos + 12);
  }

  void maybe_finish_block() {
    if (block_len > MAX_BLOCK_SIZE) {
      finish_block();
    }
  }
  
  StreamCipher *cipher;

  uint8_t * buf_start;
  uint8_t *buf_pos;
  uint32_t row_upper_bound;

  uint32_t block_num_rows;
  uint32_t block_len;

  uint32_t opcode;
  uint32_t part;
};

#include "NewInternalTypes.tcc"

#endif
