// -*- c-basic-offset: 2; fill-column: 100 -*-

#include "InternalTypes.h"
#include "Join.h"
#include "util.h"

class ProjectAttributes;
void printf(const char *fmt, ...);
void check(const char* message, bool test);

#ifndef NEW_INTERNAL_TYPES_H
#define NEW_INTERNAL_TYPES_H

bool attrs_equal(const uint8_t *a, const uint8_t *b);
uint32_t copy_attr(uint8_t *dst, const uint8_t *src);
template<typename Type>
uint32_t write_attr(uint8_t *output, Type value);
template<>
uint32_t write_attr<uint32_t>(uint8_t *output, uint32_t value);
template<>
uint32_t write_attr<float>(uint8_t *output, float value);

template<typename Type>
uint32_t read_attr(uint8_t *input, uint8_t *value);
template<>
uint32_t read_attr<uint32_t>(uint8_t *input, uint8_t *value);
template<>
uint32_t read_attr<float>(uint8_t *input, uint8_t *value);

uint32_t read_attr_internal(uint8_t *input, uint8_t *value, uint8_t expected_type);

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

  NewRecord(uint32_t upper_bound) : row_length(0) {
    row = (uint8_t *) malloc(upper_bound);
  }

  ~NewRecord() {
    free(row);
  }

  void set(NewRecord *other);

  /** Read and decrypt an encrypted row into this record. Return the number of bytes read. */
  uint32_t read_encrypted(uint8_t *input);

  /** Read a plaintext row into this record. Return the number of bytes read. */
  uint32_t read_plaintext(uint8_t *input);

  /** Encrypt and write out this record, returning the number of bytes written. */
  uint32_t write_encrypted(uint8_t *output);

  /** Write out this record in plaintext. Return the number of bytes written. */
  uint32_t write_decrypted(uint8_t *output);

  /**
   * Get a pointer to the attribute at the specified index (1-indexed). The pointer will begin at
   * the attribute type.
   */
  const uint8_t *get_attr(uint32_t attr_idx) const;

  /**
   * Get a pointer to the attribute at the specified index (1-indexed). The pointer will begin at
   * the attribute value.
   */
  const uint8_t *get_attr_value(uint32_t attr_idx) const;

  /** Append an attribute to the record. */
  template <typename AttrGeneratorType>
  void add_attr(AttrGeneratorType *attr);

  /** Mark this record as a dummy by setting all its types to dummy types. */
  void mark_dummy(uint8_t *types, uint32_t num_cols);

  void print();

  uint32_t num_cols() const {
    return *( (uint32_t *) row);
  }

  uint8_t *row;
  uint32_t row_length;
};

/**
 * A record with a projection function applied. Data that is read and subsequently written out will
 * pass through the projection function, which is specified using op_code.
 */
class NewProjectRecord {
public:
  NewProjectRecord(int op_code) : r(), op_code(op_code), project_attributes(NULL) {}

  ~NewProjectRecord();

  /** Read, decrypt, and evaluate an encrypted row. Return the number of bytes read. */
  uint32_t read_encrypted(uint8_t *input);

  /** Encrypt and write out the projected record, returning the number of bytes written. */
  uint32_t write_encrypted(uint8_t *output);

private:
  void set_project_attributes();

  NewRecord r;
  int op_code;
  ProjectAttributes *project_attributes;
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

  NewJoinRecord() : join_attr() {
    row = (uint8_t *) malloc(JOIN_ROW_UPPER_BOUND);
  }

  ~NewJoinRecord() {
    free(row);
  }

  /** Read and decrypt an encrypted row into this record. Return the number of bytes read. */
  uint32_t read_encrypted(uint8_t *input);

  /** Convert a standard record into a join record. */
  void set(bool is_primary, NewRecord *record);

  /** Copy the contents of other into this. */
  void set(NewJoinRecord *other);

  /** Encrypt and write out the record, returning the number of bytes written. */
  uint32_t write_encrypted(uint8_t *output);

  /**
   * Given two join rows, concatenate their fields into merge, dropping the join attribute from the
   * foreign row. The attribute to drop (secondary_join_attr) is specified as a 1-indexed column
   * number from the foreign row.
   */
  void merge(NewJoinRecord *other, uint32_t secondary_join_attr, NewRecord *merge);

  /** Read the join attribute from the row data into join_attr. */
  void init_join_attribute(int op_code);

  /** Return true if the record belongs to the primary table based on its table ID. */
  bool is_primary();

  /** Return true if the record contains all zeros, indicating a dummy record. */
  bool is_dummy();

  /**
   * Zero out the contents of this record. This causes sort-merge join to treat it as a dummy
   * record.
   */
  void reset_to_dummy();

  uint32_t num_cols() {
    return *( (uint32_t *) (row + TABLE_ID_SIZE));
  }

  join_attribute join_attr;

private:
  uint8_t *row;
};

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

  void aggregate(Aggregator1 *other) {
    check("Attempted to combine partial aggregates with different grouping attributes",
          this->grouping_attrs_equal(other));
    a1.add(&other->a1);
  }

  uint32_t read_encrypted(uint8_t *input) {
    uint8_t *input_ptr = input;
    uint32_t agg_size = *reinterpret_cast<uint32_t *>(input_ptr); input_ptr += 4;
    check("Aggregator length did not equal enc_size(AGG_UPPER_BOUND)",
          agg_size == enc_size(AGG_UPPER_BOUND));
    uint8_t *tmp = (uint8_t *) malloc(AGG_UPPER_BOUND);
    decrypt(input_ptr, enc_size(AGG_UPPER_BOUND), tmp); input_ptr += enc_size(AGG_UPPER_BOUND);
    uint8_t *tmp_ptr = tmp;
    num_distinct = *reinterpret_cast<uint32_t *>(tmp_ptr); tmp_ptr += 4;
    offset = *reinterpret_cast<uint32_t *>(tmp_ptr); tmp_ptr += 4;
    g.read_plaintext(tmp_ptr); tmp_ptr += ROW_UPPER_BOUND;
    tmp_ptr += a1.read_plaintext(tmp_ptr);
    free(tmp);
    return input_ptr - input;
  }

  uint32_t write_encrypted(uint8_t *output) {
    uint8_t *tmp = (uint8_t *) malloc(AGG_UPPER_BOUND);
    uint8_t *tmp_ptr = tmp;
    *reinterpret_cast<uint32_t *>(tmp_ptr) = num_distinct; tmp_ptr += 4;
    *reinterpret_cast<uint32_t *>(tmp_ptr) = offset; tmp_ptr += 4;
    g.write_whole_row_plaintext(tmp_ptr); tmp_ptr += ROW_UPPER_BOUND;
    tmp_ptr += a1.write_result(tmp_ptr);

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

  bool grouping_attrs_equal(Aggregator1 *other) {
    return g.equals(&other->g);
  }

  bool grouping_attrs_equal(NewRecord *record) {
    GroupByType g2(record);
    return g.equals(&g2);
  }

private:
  uint32_t num_distinct;
  uint32_t offset;
  GroupByType g;
  Agg1Type a1;
};

template<typename GroupByType, typename Agg1Type, typename Agg2Type>
class Aggregator2 {
public:
  Aggregator2() : num_distinct(0), g(), a1(), a2() {}

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
    check("Attempted to combine partial aggregates with different grouping attributes",
          this->grouping_attrs_equal(other));
    a1.add(&other->a1);
    a2.add(&other->a2);
  }

  uint32_t read_encrypted(uint8_t *input) {
    uint8_t *input_ptr = input;
    uint32_t agg_size = *reinterpret_cast<uint32_t *>(input_ptr); input_ptr += 4;
    check("Aggregator length did not equal enc_size(AGG_UPPER_BOUND)",
          agg_size == enc_size(AGG_UPPER_BOUND));
    uint8_t *tmp = (uint8_t *) malloc(AGG_UPPER_BOUND);
    decrypt(input_ptr, enc_size(AGG_UPPER_BOUND), tmp); input_ptr += enc_size(AGG_UPPER_BOUND);
    uint8_t *tmp_ptr = tmp;
    num_distinct = *reinterpret_cast<uint32_t *>(tmp_ptr); tmp_ptr += 4;
    offset = *reinterpret_cast<uint32_t *>(tmp_ptr); tmp_ptr += 4;
    g.read_plaintext(tmp_ptr); tmp_ptr += ROW_UPPER_BOUND;
    tmp_ptr += a1.read_plaintext(tmp_ptr);
    tmp_ptr += a2.read_plaintext(tmp_ptr);
    free(tmp);
    return input_ptr - input;
  }

  uint32_t write_encrypted(uint8_t *output) {
    uint8_t *tmp = (uint8_t *) malloc(AGG_UPPER_BOUND);
    uint8_t *tmp_ptr = tmp;
    *reinterpret_cast<uint32_t *>(tmp_ptr) = num_distinct; tmp_ptr += 4;
    *reinterpret_cast<uint32_t *>(tmp_ptr) = offset; tmp_ptr += 4;
    g.write_whole_row_plaintext(tmp_ptr); tmp_ptr += ROW_UPPER_BOUND;
    tmp_ptr += a1.write_result(tmp_ptr);
    tmp_ptr += a2.write_result(tmp_ptr);

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
    GroupByType g2(record);
    return g.equals(&g2);
  }

private:
  uint32_t num_distinct;
  uint32_t offset;
  GroupByType g;
  Agg1Type a1;
  Agg2Type a2;
};

template<uint32_t Column>
class GroupBy {
public:
  GroupBy() : row(), attr(NULL) {}
  GroupBy(NewRecord *record) : row() {
    row.set(record);
    attr = row.get_attr(Column);
  }
  /** Read an entire row and extract the grouping columns. */
  uint32_t read_plaintext(uint8_t *input) {
    uint32_t result = row.read_plaintext(input);
    this->attr = row.get_attr(Column);
    return result;
  }
  bool equals(GroupBy *other) {
    if (this->attr != NULL && other->attr != NULL) {
      return attrs_equal(this->attr, other->attr);
    } else {
      return false;
    }
  }
  void set(GroupBy *other) {
    row.set(&other->row);
    attr = row.get_attr(Column);
  }
  uint32_t write_grouping_attr(uint8_t *output) {
    return copy_attr(output, attr);
  }
  uint32_t write_whole_row_plaintext(uint8_t *output) {
    return row.write_decrypted(output);
  }
private:
  NewRecord row;
  const uint8_t *attr; // pointer into row
};

template<uint32_t Column, typename Type>
class Sum {
public:
  Sum() : sum() {}
  void set(Sum *other) {
    this->sum = other->sum;
  }
  /** Read a single attribute and set the current value to it. */
  uint32_t read_plaintext(uint8_t *input) {
    return read_attr<Type>(input, reinterpret_cast<uint8_t *>(&sum));
  }
  void zero() {
    sum = Type();
  }
  void add(NewRecord *record) {
    sum += *reinterpret_cast<const Type *>(record->get_attr_value(Column));
  }
  void add(Sum *other) {
    sum += other->sum;
  }
  uint32_t write_result(uint8_t *output) {
    return write_attr<Type>(output, sum);
  }
private:
  Type sum;
};

template<uint32_t Column, typename Type>
class Avg {
public:
  Avg() : sum(), count(0) {}
  /** Read two attributes and set the current value to them. */
  uint32_t read_plaintext(uint8_t *input) {
    uint8_t *input_ptr = input;
    input_ptr += read_attr<Type>(input_ptr, reinterpret_cast<uint8_t *>(&sum));
    input_ptr += read_attr<uint32_t>(input_ptr, reinterpret_cast<uint8_t *>(&count));
    return input_ptr - input;
  }
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
  uint32_t write_result(uint8_t *output) {
    uint8_t *output_ptr = output;
    output_ptr += write_attr<Type>(output_ptr, sum);
    output_ptr += write_attr<uint32_t>(output_ptr, count);
    return output_ptr - output;
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
  RowReader(uint8_t *buf) : buf(buf) {}

  void read(NewRecord *row) {
    buf += row->read_encrypted(buf);
  }
  void read(NewProjectRecord *row) {
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
 * After writing all rows, make sure to call close(). This currently does nothing but eventually
 * will encrypt all written rows at once.
 */
class RowWriter {
public:
  RowWriter(uint8_t *buf) : buf_start(buf), buf(buf) {}

  void write(NewRecord *row) {
    buf += row->write_encrypted(buf);
  }
  void write(NewProjectRecord *row) {
    buf += row->write_encrypted(buf);
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
