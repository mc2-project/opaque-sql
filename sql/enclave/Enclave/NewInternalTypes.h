// -*- c-basic-offset: 2; fill-column: 100 -*-

#include "util.h"

class NewJoinRecord;

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
template<>
uint32_t write_attr<uint32_t>(uint8_t *output, uint32_t value, bool dummy);
template<>
uint32_t write_attr<float>(uint8_t *output, float value, bool dummy);

template<typename Type>
uint32_t read_attr(uint8_t *input, uint8_t *value);
template<>
uint32_t read_attr<uint32_t>(uint8_t *input, uint8_t *value);
template<>
uint32_t read_attr<float>(uint8_t *input, uint8_t *value);

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
  friend class NewJoinRecord;

public:
  NewRecord() : NewRecord(ROW_UPPER_BOUND) {}

  NewRecord(uint32_t upper_bound) : row_length(4) {
    row = (uint8_t *) calloc(upper_bound, sizeof(uint8_t));
  }

  ~NewRecord() {
    free(row);
  }

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

  /** Read and decrypt an encrypted row into this record. Return the number of bytes read. */
  uint32_t read_encrypted(uint8_t *input);

  /** Write out this record in plaintext. Return the number of bytes written. */
  uint32_t write(uint8_t *output) const;

  /** Encrypt and write out this record, returning the number of bytes written. */
  uint32_t write_encrypted(uint8_t *output);

  bool less_than(const NewRecord *other, int op_code) const;

  uint32_t get_key_prefix(int op_code) const;

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
   * Append an attribute to the record.
   */
  void add_attr(uint8_t type, uint32_t len, const uint8_t *value);

  /**
   * Append an attribute to the record. The AttrGeneratorType must have a method with the following
   * signature:
   *
   *     uint32_t write_result(uint8_t *output) const;
   */
  template<typename AttrGeneratorType>
  void add_attr(const AttrGeneratorType *attr);

  /**
   * Append an attribute to the record. The AttrGeneratorType must have a method with the following
   * signature:
   *
   *     uint32_t write_result(uint8_t *output, bool dummy) const;
   */
  template<typename AttrGeneratorType>
  void add_attr(const AttrGeneratorType *attr, bool dummy);

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
 * The table ID is stored in the first 8 bytes of the row, after which is a row in the standard
 * format (see NewRecord).
 *
 * This record type can optionally provide access to a join attribute, which is a specific attribute
 * from each primary and foreign row on which the join is performed. To access the join attribute,
 * first call init_join_attribute with an opcode specifying the position of the join attribute, then
 * use join_attr.
 */
class NewJoinRecord {
public:
  static constexpr uint8_t *primary_id = (uint8_t *) "aaaaaaaa";
  static constexpr uint8_t *foreign_id = (uint8_t *) "bbbbbbbb";

  NewJoinRecord() : join_attr(NULL) {
    row = (uint8_t *) calloc(JOIN_ROW_UPPER_BOUND, sizeof(uint8_t));
  }

  ~NewJoinRecord() {
    free(row);
  }

  /** Read a plaintext row into this record. Return the number of bytes read. */
  uint32_t read(uint8_t *input);

  /** Read and decrypt an encrypted row into this record. Return the number of bytes read. */
  uint32_t read_encrypted(uint8_t *input);

  /** Write out the record in plaintext, returning the number of bytes written. */
  uint32_t write(uint8_t *output);

  /** Encrypt and write out the record, returning the number of bytes written. */
  uint32_t write_encrypted(uint8_t *output);

/** Convert a standard record into a join record. */
  void set(bool is_primary, const NewRecord *record);

  /** Copy the contents of other into this. */
  void set(NewJoinRecord *other);

  bool less_than(const NewJoinRecord *other, int op_code) const;

  uint32_t get_key_prefix(int op_code) const;

  /**
   * Given two join rows, concatenate their fields into merge, dropping the join attribute from the
   * foreign row. The attribute to drop (secondary_join_attr) is specified as a 1-indexed column
   * number from the foreign row.
   */
  void merge(const NewJoinRecord *other, uint32_t secondary_join_attr, NewRecord *merge) const;

  /** Read the join attribute from the row data into join_attr. */
  void init_join_attribute(int op_code);

  /** Return true if both records have the same join attribute. */
  bool join_attr_equals(const NewJoinRecord *other) const;

  /**
   * Get a pointer to the attribute at the specified index (1-indexed). The pointer will begin at
   * the attribute type.
   */
  const uint8_t *get_attr(uint32_t attr_idx) const;

  /** Return true if the record belongs to the primary table based on its table ID. */
  bool is_primary() const;

  /** Return true if the record contains all zeros, indicating a dummy record. */
  bool is_dummy() const;

  /**
   * Zero out the contents of this record. This causes sort-merge join to treat it as a dummy
   * record.
   */
  void reset_to_dummy();

  uint32_t num_cols() const {
    return *( (const uint32_t *) (row + TABLE_ID_SIZE));
  }

  void print() const {
    NewRecord rec;
    rec.read(row + TABLE_ID_SIZE);
    printf("JoinRecord[table=%s, row=", is_primary() ? "primary" : "foreign");
    rec.print();
    printf("]\n");
  }

private:
  uint8_t *row;
  const uint8_t *join_attr; // pointer into row
};

template<typename RecordType>
class SortPointer {
  friend class RowWriter;
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
  bool less_than(const SortPointer *other, int op_code, uint32_t *num_deep_comparisons) const {
    if (key_prefix < other->key_prefix) {
      return true;
    } else if (key_prefix > other->key_prefix) {
      return false;
    } else {
      (*num_deep_comparisons)++;
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
    record->add_attr(&g);
    record->add_attr(&a1, dummy);
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
    record->add_attr(&g);
    record->add_attr(&a1, dummy);
    record->add_attr(&a2, dummy);
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

  /** Write the grouping attribute as plaintext to output and return the number of bytes written. */
  uint32_t write_grouping_attr(uint8_t *output) {
    return copy_attr(output, attr);
  }

  /** Write the grouping attribute as plaintext to output and return the number of bytes written. */
  uint32_t write_result(uint8_t *output) const {
    return copy_attr(output, attr);
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
 * Holds state for an ongoing sum aggregation operation. The column to sum is selected by specifying
 * Column (1-indexed) and the type of that column is specified using Type. Supports resetting and
 * aggregating (methods zero and add), reading/writing partial aggregation state (methods
 * read_partial_result and write_partial_result), and writing the final aggregation result (method
 * write_result).
 */
template<uint32_t Column, typename Type>
class Sum {
public:
  Sum() : sum() {}

  /** Update the sum to an arbitrary value. */
  void set(Sum *other) {
    this->sum = other->sum;
  }

  /** Reset the sum to zero. */
  void zero() {
    sum = Type();
  }

  /** Add in the value from a single record. */
  void add(NewRecord *record) {
    sum += *reinterpret_cast<const Type *>(record->get_attr_value(Column));
  }

  /** Combine the value from another Sum object. */
  void add(Sum *other) {
    sum += other->sum;
  }

  /** Read a partial sum (one plaintext attribute) and return the number of bytes read. */
  uint32_t read_partial_result(uint8_t *input) {
    return read_attr<Type>(input, reinterpret_cast<uint8_t *>(&sum));
  }

  /** Write the partial sum as a single plaintext attribute and return num bytes written. */
  uint32_t write_partial_result(uint8_t *output) {
    return write_result(output, false);
  }

  /** Write the final sum as a single plaintext attribute and return num bytes written. */
  uint32_t write_result(uint8_t *output, bool dummy) const {
    return write_attr<Type>(output, sum, dummy);
  }

  void print() {
    printf("Sum[sum=%f]\n", static_cast<float>(sum));
  }

private:
  Type sum;
};

/**
 * Holds state for an ongoing average (mean) aggregation operation. See Sum.
 */
template<uint32_t Column, typename Type>
class Avg {
public:
  Avg() : sum(), count(0) {}

  void set(Avg *other) {
    this->sum = other->sum;
    this->count = other->count;
  }

  void zero() {
    sum = Type();
    count = 0;
  }

  void add(NewRecord *record) {
    sum += *reinterpret_cast<const Type *>(record->get_attr_value(Column));
    count++;
  }

  void add(Avg *other) {
    sum += other->sum;
    count += other->count;
  }

  /** Read a partial average (two plaintext attributes: sum and count) and return num bytes read. */
  uint32_t read_partial_result(uint8_t *input) {
    uint8_t *input_ptr = input;
    input_ptr += read_attr<Type>(input_ptr, reinterpret_cast<uint8_t *>(&sum));
    input_ptr += read_attr<uint32_t>(input_ptr, reinterpret_cast<uint8_t *>(&count));
    return input_ptr - input;
  }

  /** Write the partial average (two plaintext attrs: sum and count); return num bytes written. */
  uint32_t write_partial_result(uint8_t *output) {
    uint8_t *output_ptr = output;
    output_ptr += write_attr<Type>(output_ptr, sum, false);
    output_ptr += write_attr<uint32_t>(output_ptr, count, false);
    return output_ptr - output;
  }

  /** Write the final average as one plaintext attr of type Type; return num bytes written. */
  uint32_t write_result(uint8_t *output, bool dummy) const {
    double avg = static_cast<double>(sum) / static_cast<double>(count);
    uint8_t *output_ptr = output;
    output_ptr += write_attr<Type>(output_ptr, static_cast<Type>(avg), dummy);
    return output_ptr - output;
  }

  void print() {
    printf("Avg[sum=%f, count=%d]\n", sum, count);
  }

private:
  Type sum;
  uint32_t count;
};

/**
 * Manages reading multiple encrypted rows from a buffer.
 *
 * To read rows, initialize an empty row object and repeatedly call the appropriate read function
 * with it, which will populate the row object with the next row.
 *
 * This class performs no bounds checking; the caller is responsible for knowing how many rows the
 * buffer contains.
 */
class RowReader {
public:
  RowReader(uint8_t *buf) : buf(buf) {
    block_start = (uint8_t *) malloc(MAX_BLOCK_SIZE);
    read_encrypted_block();
  }

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

private:
  void read_encrypted_block() {
    uint32_t block_enc_size = *reinterpret_cast<uint32_t *>(buf); buf += 4;
    block_num_rows = *reinterpret_cast<uint32_t *>(buf); buf += 4;
    decrypt(buf, block_enc_size, block_start);
    buf += block_enc_size;
    block_pos = block_start;
    block_rows_read = 0;
  }

  void maybe_advance_block() {
    if (block_rows_read >= block_num_rows) {
      read_encrypted_block();
    }
  }

  uint8_t *buf;

  uint8_t *block_start;
  uint8_t *block_pos;
  uint32_t block_num_rows;
  uint32_t block_rows_read;
};

class IndividualRowReader {
public:
  IndividualRowReader(uint8_t *buf) : buf(buf) {}

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
 * Manages encrypting and writing out multiple rows to an output buffer.
 *
 * After writing all rows, make sure to call close().
 */
class RowWriter {
public:
  RowWriter(uint8_t *buf)
    : buf_start(buf), buf_pos(buf), block_num_rows(0), block_padded_len(0) {
    block_start = (uint8_t *) malloc(MAX_BLOCK_SIZE);
    block_pos = block_start;
  }

  ~RowWriter() {
    free(block_start);
  }

  void write(NewRecord *row) {
    maybe_finish_block(ROW_UPPER_BOUND);
    uint32_t delta = row->write(block_pos);
    check(delta <= ROW_UPPER_BOUND,
          "Wrote %d, which is more than ROW_UPPER_BOUND\n", delta);
    block_pos += delta;
    block_num_rows++;
    block_padded_len += ROW_UPPER_BOUND;
  }

  void write(NewJoinRecord *row) {
    maybe_finish_block(JOIN_ROW_UPPER_BOUND);
    block_pos += row->write(block_pos);
    block_num_rows++;
    block_padded_len += JOIN_ROW_UPPER_BOUND;
  }

  template<typename RecordType>
  void write(SortPointer<RecordType> *ptr) {
    write(ptr->rec);
  }

  void finish_block() {
    *reinterpret_cast<uint32_t *>(buf_pos) = enc_size(MAX_BLOCK_SIZE); buf_pos += 4;
    *reinterpret_cast<uint32_t *>(buf_pos) = block_num_rows; buf_pos += 4;
    encrypt(block_start, MAX_BLOCK_SIZE, buf_pos);
    buf_pos += enc_size(MAX_BLOCK_SIZE);

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

  uint8_t *block_start;
  uint8_t *block_pos;
  uint32_t block_num_rows;
  uint32_t block_padded_len;
};

class IndividualRowWriter {
public:
  IndividualRowWriter(uint8_t *buf) : buf_start(buf), buf(buf) {}

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

  void close() {}

  uint32_t bytes_written() {
    return buf - buf_start;
  }

private:
  uint8_t * const buf_start;
  uint8_t *buf;
};

#include "NewInternalTypes.tcc"

#endif
