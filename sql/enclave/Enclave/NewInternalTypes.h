// -*- c-basic-offset: 2; fill-column: 100 -*-

#include "InternalTypes.h"

#ifndef NEW_INTERNAL_TYPES_H
#define NEW_INTERNAL_TYPES_H

class ProjectAttributes;

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

  uint32_t num_cols() {
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

class NewJoinRecord {
public:
  NewJoinRecord() : row_length(0) {
    row = (uint8_t *) malloc(JOIN_ROW_UPPER_BOUND);
  }

  ~NewJoinRecord() {
    free(row);
  }

  void set(bool is_primary, NewRecord *record);

  uint32_t write_encrypted(uint8_t *output);

  uint32_t num_cols() {
    return *( (uint32_t *) (row + TABLE_ID_SIZE));
  }

private:
  uint8_t *row;
  uint32_t row_length;
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

  void close() {}

  uint32_t bytes_written() {
    return buf - buf_start;
  }

private:
  uint8_t * const buf_start;
  uint8_t *buf;
};

#endif
