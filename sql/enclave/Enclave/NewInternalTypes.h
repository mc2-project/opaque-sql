// -*- c-basic-offset: 2 -*-

#include "InternalTypes.h"

#ifndef NEW_INTERNAL_TYPES_H
#define NEW_INTERNAL_TYPES_H

class ProjectAttributes;

class NewRecord {
public:
  NewRecord() : NewRecord(ROW_UPPER_BOUND) {}

  NewRecord(uint32_t upper_bound) {
    row = (uint8_t *) malloc(upper_bound);
  }

  ~NewRecord() {
    free(row);
  }

  virtual uint32_t read(uint8_t *input);
  virtual uint32_t write_encrypted(uint8_t *output);
  virtual uint32_t write_decrypted(uint8_t *output);

  virtual uint32_t num_cols() {
    return *( (uint32_t *) row);
  }

protected:
  uint8_t *row;
  uint32_t row_length;
};

class NewProjectRecord : public NewRecord {
public:
  NewProjectRecord(int op_code) : NewRecord(), op_code(op_code), project_attributes(NULL) {}

  ~NewProjectRecord();

  virtual uint32_t read(uint8_t *input);
  virtual uint32_t write_encrypted(uint8_t *output);

private:
  void set_project_attributes();

  int op_code;
  ProjectAttributes *project_attributes;
};

class NewJoinRecord : public NewRecord {
public:
  NewJoinRecord()
    : NewRecord(JOIN_ROW_UPPER_BOUND) {}

  void set(bool is_primary, NewRecord *record);

  virtual uint32_t write_encrypted(uint8_t *output);

  virtual uint32_t num_cols() {
    return *( (uint32_t *) (row + TABLE_ID_SIZE));
  }
};

class RowReader {
public:
  RowReader(uint8_t *buf) : buf(buf) {}

  void read(NewRecord *row) {
    buf += row->read(buf);
  }

private:
  uint8_t *buf;
};

class RowWriter {
public:
  RowWriter(uint8_t *buf) : buf_start(buf), buf(buf) {}

  void write(NewRecord *row) {
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
