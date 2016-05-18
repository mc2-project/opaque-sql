// -*- c-basic-offset: 2; fill-column: 100 -*-

#include "InternalTypes.h"
#include "Join.h"
#include "util.h"

#ifndef NEW_INTERNAL_TYPES_H
#define NEW_INTERNAL_TYPES_H

class ProjectAttributes;
void printf(const char *fmt, ...);

bool attrs_equal(const uint8_t *a, const uint8_t *b);
uint32_t copy_attr(uint8_t *dst, const uint8_t *src);
template<typename Type>
uint32_t write_attr(uint8_t *output, Type value);
template<>
uint32_t write_attr<uint32_t>(uint8_t *output, uint32_t value);
template<>
uint32_t write_attr<float>(uint8_t *output, float value);

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

  /** Read and decrypt an encrypted row into this record. Return the number of bytes read. */
  uint32_t read(uint8_t *input);

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
  uint32_t read(uint8_t *input);

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
  uint32_t read(uint8_t *input);

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
  Aggregator1() : num_distinct(0), g(), a1() {}

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

  uint32_t write_encrypted(NewRecord *record, uint8_t *output) {
    uint8_t *tmp = (uint8_t *) malloc(AGG_UPPER_BOUND);
    uint8_t *tmp_ptr = tmp;
    *reinterpret_cast<uint32_t *>(tmp_ptr) = num_distinct; tmp_ptr += 4;
    *reinterpret_cast<uint32_t *>(tmp_ptr) = num_distinct - 1; tmp_ptr += 4;
    record->write_decrypted(tmp_ptr); tmp_ptr += ROW_UPPER_BOUND;
    tmp_ptr += a1.write_result(tmp_ptr);

    uint8_t *output_ptr = output;
    *reinterpret_cast<uint32_t *>(output_ptr) = enc_size(AGG_UPPER_BOUND); output_ptr += 4;
    encrypt(tmp, AGG_UPPER_BOUND, output_ptr); output_ptr += enc_size(AGG_UPPER_BOUND);
    return output_ptr - output;
  }

private:
  uint32_t num_distinct;
  GroupByType g;
  Agg1Type a1;
};

template<typename GroupByType, typename Agg1Type, typename Agg2Type>
class Aggregator2 {
public:
  Aggregator2() : num_distinct(0), g(), a1(), a2() {}

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

  uint32_t write_encrypted(NewRecord *record, uint8_t *output) {
    uint8_t *tmp = (uint8_t *) malloc(AGG_UPPER_BOUND);
    uint8_t *tmp_ptr = tmp;
    *reinterpret_cast<uint32_t *>(tmp_ptr) = num_distinct; tmp_ptr += 4;
    *reinterpret_cast<uint32_t *>(tmp_ptr) = num_distinct - 1; tmp_ptr += 4;
    record->write_decrypted(tmp_ptr); tmp_ptr += ROW_UPPER_BOUND;
    tmp_ptr += a1.write_result(tmp_ptr);
    tmp_ptr += a2.write_result(tmp_ptr);

    uint8_t *output_ptr = output;
    *reinterpret_cast<uint32_t *>(output_ptr) = enc_size(AGG_UPPER_BOUND); output_ptr += 4;
    encrypt(tmp, AGG_UPPER_BOUND, output_ptr); output_ptr += enc_size(AGG_UPPER_BOUND);
    return output_ptr - output;
  }

private:
  uint32_t num_distinct;
  GroupByType g;
  Agg1Type a1;
  Agg2Type a2;
};

template<uint32_t Column>
class GroupBy {
public:
  GroupBy() : attr(NULL) {}
  GroupBy(NewRecord *record) : attr(record->get_attr(Column)) {}
  bool equals(GroupBy *other) {
    if (this->attr != NULL && other->attr != NULL) {
      return attrs_equal(this->attr, other->attr);
    } else {
      return false;
    }
  }
  void set(GroupBy *other) {
    this->attr = other->attr;
  }
  uint32_t write_result(uint8_t *output) {
    return copy_attr(output, attr);
  }
private:
  const uint8_t *attr;
};

template<uint32_t Column, typename Type>
class Sum {
public:
  Sum() : sum() {}
  void zero() {
    sum = Type();
  }
  void add(NewRecord *record) {
    sum += *reinterpret_cast<const Type *>(record->get_attr_value(Column));
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
  void zero() {
    sum = Type();
    count = 0;
  }
  void add(NewRecord *record) {
    sum += *reinterpret_cast<const Type *>(record->get_attr_value(Column));
    count++;
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
    buf += row->read(buf);
  }
  void read(NewProjectRecord *row) {
    buf += row->read(buf);
  }
  void read(NewJoinRecord *row) {
    buf += row->read(buf);
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
  void write(AggregatorType *agg, NewRecord *row) {
    buf += agg->write_encrypted(row, buf);
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
