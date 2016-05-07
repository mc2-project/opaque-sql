#include "Join.h"


uint32_t encrypt_and_write_row(uint8_t *input_row_ptr,
			       uint8_t *output_row_ptr) {
  // write out num_cols
  // printf("Encrypting and formatting row\n");
  uint32_t num_cols = *( (uint32_t *) input_row_ptr);

  // printf("num cols is %u\n", num_cols);
  uint32_t value_len = 0;
  *((uint32_t *) output_row_ptr) = num_cols;
  
  uint32_t input_offset = 4;
  uint32_t output_offset = 4;

  uint8_t *input_row_ptr_ = input_row_ptr + 4;
  uint8_t *output_row_ptr_ = output_row_ptr + 4;
  
  for (uint32_t i = 0; i < num_cols; i++) {
    encrypt_attribute(&input_row_ptr_, &output_row_ptr_);
  }

  return (output_row_ptr_ - output_row_ptr);;
}

// This pre-processing pads all rows to ROW_UPPER_BOUND, and then encrypt the entire thing
// This is necessary because we do not want to leak the distribution of table 1 and table 2 after
// the sort operation
// Also adds a tabld ID for indicating whether this row belongs to the primary key table
void join_sort_preprocess(int op_code,
			  uint8_t *table_id, 
			  uint8_t *input_row, uint32_t input_row_len,
			  uint8_t *output_row, uint32_t output_row_len) {

  uint8_t temp[JOIN_ROW_UPPER_BOUND];
  uint8_t *temp_ptr = temp;
  // decrypt each attribute, copy to temp, then encrypt the entire row to output_row
  // (including the number of columns!)

  uint8_t primary_table[TABLE_ID_SIZE];
  uint8_t foreign_table[TABLE_ID_SIZE];

  get_table_indicator(primary_table, foreign_table);

  //printf("join sort preprocess called, input size %u, output_row_len %u, join_row_upperbound: %u\n", input_row_len, output_row_len, JOIN_ROW_UPPER_BOUND);
	
  int if_primary = 0;
  
  if (op_code == OP_JOIN_COL1 || op_code == OP_JOIN_COL2) {
    char cmp_table[TABLE_ID_SIZE+1] = "aaaaaaaa";
    if_primary = cmp(table_id, (uint8_t *) cmp_table, TABLE_ID_SIZE);
  } else {
    printf("join_sort_preprocess: Unknown opcode %d\n", op_code);
    assert(false);
  }

  if (if_primary == 0) {
    cpy(temp_ptr, primary_table, TABLE_ID_SIZE);
  } else {
    cpy(temp_ptr, foreign_table, TABLE_ID_SIZE);
  }

  temp_ptr += TABLE_ID_SIZE;

  uint8_t *enc_row_ptr = input_row;
  uint32_t enc_row_len = 0;

  uint8_t *enc_value_ptr = NULL;
  uint32_t enc_value_len = 0;

  uint32_t num_cols = *( (uint32_t *) enc_row_ptr);
  enc_row_ptr += 4;
  enc_value_ptr = enc_row_ptr;

  *( (uint32_t *) temp_ptr) = num_cols;
  temp_ptr += 4;
  
  //printf("if_primary: %d, num_cols: %u\n", if_primary, num_cols);
  
  for (uint32_t i = 0; i < num_cols; i++) {
    // find_attribute(enc_row_ptr, enc_row_len, num_cols,
    // 			   i + 1, &enc_value_ptr, &enc_value_len);

    // decrypt(enc_value_ptr, enc_value_len, temp_ptr);
    // temp_ptr += dec_size(enc_value_len);

    decrypt_attribute(&enc_value_ptr, &temp_ptr);
  }

  //printf("decrypted all\n");

  uint32_t total_len = (uint32_t) (temp_ptr - temp);

  //print_join_row("Join preprocess", temp);

  encrypt(temp, JOIN_ROW_UPPER_BOUND, output_row);

  //printf("encrypted all\n");
}

class join_attribute {
public:
  join_attribute() {
	length_ = 0;
	buffer_ptr = buffer;
  }

  // copy attribute to buffer, and increase buffer pointer
  // also increase length_
  // the attribute length should be
  void new_attribute(uint8_t *attr, uint32_t attr_len) {
	cpy(buffer_ptr, attr, attr_len);
	buffer_ptr += attr_len;
	length_ += attr_len;
  }

  uint32_t get_length() {
	return length_;
  }

  void reset_pointer() {
	buffer_ptr = buffer;
  }
  
  // reset everything
  void reset() {
	this->reset_pointer();
	this->length_ = 0;
  }

  int compare(join_attribute *attr) {
	if (attr->length_ != this->length_) {
	  return -1;
	}

	return cmp(this->buffer, attr->buffer, this->length_);
  }

  int compare(uint8_t *attr, uint32_t attr_len) {
	if (this->length_ != attr_len) {
	  return -1;
	}

	return cmp(this->buffer, attr, attr_len);
  }

  // copy from another join_attribute
  void copy_attribute(join_attribute *attr) {
	cpy(this->buffer, attr->buffer, ROW_UPPER_BOUND);
	this->length_ = attr->get_length();
	this->reset_pointer();
  }

  uint32_t length_;
  uint8_t *buffer_ptr;
  uint8_t buffer[ROW_UPPER_BOUND];
};

// given a decrypted row and an opcode, extract the join attribute
void get_join_attribute(int op_code,
			uint32_t num_cols, uint8_t *row,
			int if_primary,
			join_attribute *join_attr) {
  join_attr->reset();
  uint8_t *row_ptr = row;
  uint32_t total_value_len = 0;
  uint32_t join_attr_idx;
  
  if (op_code == OP_JOIN_COL1) {
    join_attr_idx = 1;
  } else if (op_code == OP_JOIN_COL2) {
    join_attr_idx = 2;
  } else {
    printf("get_join_attribute: Unknown opcode %d\n", op_code);
    assert(false);
  }

  // Join both tables on join_attr
  for (uint32_t i = 0; i < num_cols; i++) {
    total_value_len = *( (uint32_t *) (row_ptr + TYPE_SIZE)) + TYPE_SIZE + 4;
    if (i + 1 == join_attr_idx) {
      join_attr->new_attribute(row_ptr, total_value_len);
    } else {
      // TODO: dummy write
    }
    row_ptr += total_value_len;
  }
}


// join two rows together
// based on the op_code, de-duplicate the join columns
// assume that output row has enough buffer size
void join_merge_row(int op_code,
					uint8_t *primary_row, uint8_t *secondary_row,
					uint8_t *output_row) {

  uint8_t *input_ptr = primary_row;
  uint8_t *output_row_ptr = output_row;

  uint32_t value_len = 0;

  uint32_t primary_row_cols = 0;
  uint32_t secondary_row_cols = 0;
  uint32_t secondary_join_attr = 0;
  
  if (op_code == OP_JOIN_COL1) {
    secondary_join_attr = 1;
  } else if (op_code == OP_JOIN_COL2) {
    secondary_join_attr = 2;
  } else {
    printf("join_merge_row: Unknown opcode %d\n", op_code);
    assert(false);
  }

  primary_row_cols = *( (uint32_t *) primary_row);
  secondary_row_cols =  *( (uint32_t *) secondary_row);

  *( (uint32_t *) output_row_ptr) = primary_row_cols + secondary_row_cols - 1;
  output_row_ptr += 4;

  input_ptr += 4;
  // first write out primary_row
  for (uint32_t i = 0; i < primary_row_cols; i++) {
    value_len = *( (uint32_t *) (input_ptr + TYPE_SIZE)) + HEADER_SIZE;
    cpy(output_row_ptr, input_ptr, value_len);
    input_ptr += value_len;
    output_row_ptr += value_len;
  }

  // now, write out the other row, skipping the duplicate columns
  input_ptr = secondary_row;
  input_ptr += 4;

  for (uint32_t i = 0; i < secondary_row_cols; i++) {
    value_len = *( (uint32_t *) (input_ptr + TYPE_SIZE)) + HEADER_SIZE;
    if (i + 1 != secondary_join_attr) {
      cpy(output_row_ptr, input_ptr, value_len);
      output_row_ptr += value_len;
    }
    input_ptr += value_len;
  }
}

// Join in enclave: assumes that the records have been sorted
// by the join attribute already
// This method takes in a temporay row (which could be a dummy row)
// Then it compares with the following rows (which should contain the row info, as well as the table info


// This join can be implemented by merging from one table to another
// The tables should have encrypted identifiers so that they can be identified
//
// Format of the input rows should be:
// enc{table name}{row}
//
// Output row should be a new row, except the join attributes are de-duplicated
//
// Assume that the table has been transformed into a primary key-foreign key
// join format
//
// TODO: should we leak which attributes are being joined, but not the constants?
void sort_merge_join(int op_code,
		     uint8_t *input_rows, uint32_t input_rows_length,
		     uint32_t num_rows,
		     uint8_t *join_row, uint32_t join_row_length,
                     uint8_t *output_rows, uint32_t output_rows_length,
                     uint32_t *actual_output_length) {


  // iterate through the sorted rows and output join
  // output should be 2 * ROW_UPPER_BOUND
  // there should be one output per input
  
  uint32_t agg_attribute_num = 2;
  uint32_t sort_attribute_num = 2;

  uint8_t *input_ptr = input_rows;
  uint8_t *output_rows_ptr = output_rows;

  uint8_t value_type = 0;
  uint32_t value_len = 0;
  uint8_t *value_ptr = NULL;

  // table_p is the table that is joining on primary key
  // table_f joins on foreign key
  uint8_t table_p[TABLE_ID_SIZE];
  uint8_t table_f[TABLE_ID_SIZE];
  // decrypt the table IDs
  get_table_indicator(table_p, table_f);
  
  uint8_t *current_table = NULL;

  join_attribute primary_join_attr;
  uint8_t primary_row[JOIN_ROW_UPPER_BOUND];
  uint8_t *primary_row_ptr = primary_row;
  uint32_t primary_row_len = 0;
  
  join_attribute current_join_attr;
  uint8_t current_row[JOIN_ROW_UPPER_BOUND];
  uint8_t *current_row_ptr = current_row;

  uint8_t *dummy_row = (uint8_t *) malloc(JOIN_ROW_UPPER_BOUND * 2);
  uint8_t *dummy_row_ptr = dummy_row;
  uint32_t dummy_row_len = 0;

  uint8_t *merge_row = (uint8_t *) malloc(JOIN_ROW_UPPER_BOUND * 2);

  uint32_t num_cols = 0;

  // printf("Sort merge join called\n");

  decrypt(join_row, enc_size(JOIN_ROW_UPPER_BOUND), primary_row);
  // check to see if this is a dummy row
  if (test_dummy(primary_row, JOIN_ROW_UPPER_BOUND) != 0) {
    check("primary_row != table_p", cmp(primary_row, table_p, TABLE_ID_SIZE) == 0);
    num_cols = *( (uint32_t *) (primary_row + TABLE_ID_SIZE));
    get_join_attribute(op_code, num_cols,
                       primary_row + TABLE_ID_SIZE + 4, 0,
					   &primary_join_attr);
  } else {
    // printf("Join row is a dummy!\n");
  }
  
  // construct dummy rows
  // constructs a final row with final num cols = 
  // (num cols of table P + num cols of table f)
  // each attribute will be empty, but type is specified
  if (op_code == OP_JOIN_COL2) {
    *( (uint32_t *) dummy_row_ptr) = 5;
    dummy_row_ptr += 4;
    uint8_t types[5] = {DUMMY_INT, DUMMY_STRING, DUMMY_INT, DUMMY_INT, DUMMY_INT};
    uint32_t upper_bound = 0;
	
    for (uint32_t i = 0; i < 5; i++) {
      uint8_t t = types[i];
      // instead of writing back the correct type, we need to write a dummy type
      *dummy_row_ptr = types[i];
      dummy_row_ptr += TYPE_SIZE;

      upper_bound = attr_upper_bound(t);

      *( (uint32_t *) dummy_row_ptr) = upper_bound;
      dummy_row_ptr += 4;
      dummy_row_ptr += upper_bound;
    }
  } else if (op_code == OP_JOIN_COL1) {
    *( (uint32_t *) dummy_row_ptr) = 4;
    dummy_row_ptr += 4;
    uint8_t types[4] = {DUMMY_STRING, DUMMY_INT, DUMMY_STRING, DUMMY_FLOAT};
    uint32_t upper_bound = 0;

    for (uint32_t i = 0; i < 4; i++) {
      uint8_t t = types[i];
      // instead of writing back the correct type, we need to write a dummy type
      *dummy_row_ptr = types[i];
      dummy_row_ptr += TYPE_SIZE;

      upper_bound = attr_upper_bound(t);

      *( (uint32_t *) dummy_row_ptr) = upper_bound;
      dummy_row_ptr += 4;
      dummy_row_ptr += upper_bound;
    }
  } else {
    printf("sort_merge_join: Unknown opcode %d\n", op_code);
    assert(false);
  }

  dummy_row_ptr = dummy_row;

  for (uint32_t r = 0; r < num_rows; r++) {
	// these rows are completely encrypted, need to decrypt first
	decrypt(input_ptr, enc_size(JOIN_ROW_UPPER_BOUND), current_row);
	current_row_ptr = current_row;
	
	// table ID
	current_table = current_row_ptr;
	current_row_ptr += TABLE_ID_SIZE;
	
	num_cols = *( (uint32_t *) current_row_ptr);
    // printf("Record %u, num cols is %u\n", r, num_cols);
	current_row_ptr += 4;

    // print_bytes(current_table, TABLE_ID_SIZE);

	int if_primary = cmp(table_p, current_table, TABLE_ID_SIZE);
	get_join_attribute(op_code, num_cols,
					   current_row_ptr, if_primary,
					   &current_join_attr);
    // printf("if_primary: %u\n", if_primary);
    // print_attribute("Current join attr", current_join_attr.buffer);

	if (if_primary == 0) {
	  if (primary_join_attr.compare(&current_join_attr) != 0) {
		// if the current row is in the primary table, and
		// the primary join attribute & current attribute are different,
		// we can advance to a new primary join attribute
		primary_join_attr.copy_attribute(&current_join_attr);
		cpy(primary_row, current_row + TABLE_ID_SIZE, ROW_UPPER_BOUND);
	  } else {
		// this shouldn't happen, based on the assumptions!
        check("violated assumptions?", false);
	  }
	  // write out dummy join
	  output_rows_ptr += encrypt_and_write_row(dummy_row, output_rows_ptr);
	} else {
	  if (primary_join_attr.compare(&current_join_attr) != 0) {
		// write out dummy join
		output_rows_ptr += encrypt_and_write_row(dummy_row, output_rows_ptr);
	  } else {
		// need to do a real join
		join_merge_row(op_code, primary_row, current_row + TABLE_ID_SIZE, merge_row);
		output_rows_ptr += encrypt_and_write_row(merge_row, output_rows_ptr);
		//print_row("Join row: ", merge_row);
	  }
	}

	input_ptr += enc_size(JOIN_ROW_UPPER_BOUND);
	
  }

  *actual_output_length = output_rows_ptr - output_rows;
  
  free(dummy_row);
  free(merge_row);
}



// do a scan of all of the encrypted rows
// return the last primary table row in this 
void scan_collect_last_primary(int op_code,
			       uint8_t *input_rows, uint32_t input_rows_length,
			       uint32_t num_rows,
			       uint8_t *output, uint32_t output_length) {

  // primary_table is the table that is joining on primary key
  // foreign_table joins on foreign key
  uint8_t primary_table[TABLE_ID_SIZE];
  uint8_t foreign_table[TABLE_ID_SIZE];
  get_table_indicator(primary_table, foreign_table);  

  uint8_t *current_table = NULL;


  // printf("scan_collect_last_primary called\n");
  
  uint8_t *input_rows_ptr = input_rows;
  
  join_attribute current_join_attr;
  uint8_t current_row[JOIN_ROW_UPPER_BOUND];
  uint8_t *current_row_ptr = current_row;

  uint8_t primary_row[JOIN_ROW_UPPER_BOUND];
  // clear the primary row, and write table_f's table ID in there
  // later, during the boundary record processing, it's easy to tell whether this is a dummy record
  write_dummy(primary_row, JOIN_ROW_UPPER_BOUND);
  cpy(primary_row, foreign_table, TABLE_ID_SIZE);
  
  for (uint32_t r = 0; r < num_rows; r++) {
	decrypt(input_rows_ptr, enc_size(JOIN_ROW_UPPER_BOUND), current_row);
	input_rows_ptr += enc_size(JOIN_ROW_UPPER_BOUND);
	
	current_row_ptr = current_row;

	// table ID
	current_table = current_row_ptr;
	current_row_ptr += TABLE_ID_SIZE;

	if (cmp(current_table, primary_table, TABLE_ID_SIZE) == 0) {
	  cpy(primary_row, current_row, JOIN_ROW_UPPER_BOUND);
	} else {
	  // do a dummy write
	  cpy(current_row, current_row, JOIN_ROW_UPPER_BOUND);
	  cpy(primary_row, primary_row, JOIN_ROW_UPPER_BOUND);
	}
  }

  // return the last primary row, if there is any!
  //print_join_row("Join row", primary_row);
  encrypt(primary_row, JOIN_ROW_UPPER_BOUND, output);
  // printf("enc_size is %u\n", enc_size(JOIN_ROW_UPPER_BOUND));
}


// collect and process boundary records
void process_join_boundary(int op_code,
                           uint8_t *input_rows, uint32_t input_rows_length,
						   uint32_t num_rows,
                           uint8_t *output_rows, uint32_t output_rows_size,
                           uint32_t *actual_output_length) {

  uint8_t *input_rows_ptr = input_rows;
  uint8_t *output_rows_ptr = output_rows;
  
  uint8_t prev_join_row[JOIN_ROW_UPPER_BOUND];
  uint8_t current_join_row[JOIN_ROW_UPPER_BOUND];
  write_dummy(prev_join_row, JOIN_ROW_UPPER_BOUND);
  write_dummy(current_join_row, JOIN_ROW_UPPER_BOUND);

  uint8_t *current_table = current_join_row;

  // printf("process_join_boundary called\n");

  for (uint32_t i = 0; i < num_rows; i++) {
	cpy(prev_join_row, current_join_row, JOIN_ROW_UPPER_BOUND);
	decrypt(input_rows_ptr, enc_size(JOIN_ROW_UPPER_BOUND), current_join_row);

	if (i == 0) {
	  // encrypt a dummy record
	  encrypt(prev_join_row, JOIN_ROW_UPPER_BOUND, output_rows_ptr);
	  input_rows_ptr += enc_size(JOIN_ROW_UPPER_BOUND);
	  output_rows_ptr += enc_size(JOIN_ROW_UPPER_BOUND);
	  continue;
	}

    // check the table ID
    char cmp_table[TABLE_ID_SIZE+1] = "aaaaaaaa";
    int if_primary = cmp(current_table, (uint8_t *)cmp_table, TABLE_ID_SIZE);

    if (if_primary == 0) {
	  // write out the previous join attribute
      encrypt(prev_join_row, JOIN_ROW_UPPER_BOUND, output_rows_ptr);
      // need a dummy write to current_row
      cpy(current_join_row, current_join_row, JOIN_ROW_UPPER_BOUND);
    } else {
	  // current join row is a dummy, need to copy previous row into current row
	  cpy(current_join_row, prev_join_row, JOIN_ROW_UPPER_BOUND);
	  encrypt(prev_join_row, JOIN_ROW_UPPER_BOUND, output_rows_ptr);
    }
	
	input_rows_ptr += enc_size(JOIN_ROW_UPPER_BOUND);
	output_rows_ptr += enc_size(JOIN_ROW_UPPER_BOUND);
  }

  *actual_output_length = output_rows_ptr - output_rows;
}
