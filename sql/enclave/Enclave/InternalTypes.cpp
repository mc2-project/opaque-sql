#include "InternalTypes.h"

Integer::Integer() {
  value = 0;
}

Integer::Integer(int v) {
  this->value = v;
}

int Integer::compare(GenericType *v) {
  Integer *int_v = dynamic_cast<Integer *>(v);
  
  if (this->value > int_v->value) {
	return 1;
  } else if (this->value < int_v->value) {
	return -1;
  }

  return 0;
}

void Integer::swap(GenericType *v) {
  Integer *int_v = dynamic_cast<Integer *>(v);
  
  int value_ = this->value;
  this->value = int_v->value;
  int_v->value = value_;
}
	
void Integer::compare_and_swap(Integer *v) {
  if (this->compare(v) == 1) {
	this->swap(v);
  }
}

void Integer::consume(uint8_t *input, int mode) {
  uint8_t type = *input;
  if (type == INT) {
	this->value = *( (uint32_t *) (input + HEADER_SIZE));
  }
}
  
void Integer::flush(uint8_t *output) {
  // write back type, size, and actual output
  *output = INT;
  *( (uint32_t *) output + TYPE_SIZE) = 4;
  *( (uint32_t *) output + HEADER_SIZE) = value;
}

void Integer::copy_attr(Integer *attr) {
  this->value = attr->value;
}

void Integer::print() {
  printf("Integer attr: %u\n", this->value);
}

String::String() {
  data = NULL;
  length = 0;
  if_alloc = -1;
}

String::~String() {
  if (if_alloc == 0) {
	free(data);
  }
}

// this |</>/=| str
int String::compare(GenericType *v) {
  // String comparison is lexicographic order

  String *str = dynamic_cast<String *>(v);
	
  uint32_t min_size = this->length < str->length ? str->length : str->length;
  int ret = 0;
  for (uint32_t i = 0; i < min_size; i++) {
	if (*(this->data+i) < *(str->data+i)) {
	  return -1;
	} else if (*(this->data+i) > *(str->data+i)) {
	  return 1;
	}
  }

  if (this->length < str->length) {
	return -1;
  } else if (this->length > str->length) {
	return 1;
  }

  // these two strings are exactly equal!
  return ret;
}

/* static int compare(String *attr1, String *attr2) { */
/* 	return attr1->compare(attr2); */
/* } */

void String::swap(GenericType *attr2) {
  String *attr = dynamic_cast<String *>(attr2);
  
  uint32_t length_ = this->length;
  uint8_t *data_ = this->data;
  int if_alloc_ = this->if_alloc;
  this->length = attr->length;
  this->data = attr->data;
  this->if_alloc = attr->if_alloc;
  attr->length = length_;
  attr->data = data_;
  attr->if_alloc = if_alloc_;
}

void String::alloc(uint8_t *buffer) {
  if (if_alloc == 0) {
	free(data);
  }
	
  if (buffer == NULL) {
	this->data = buffer;
	if_alloc = -1;
  } else {
	this->data = (uint8_t *) malloc(STRING_UPPER_BOUND);
	if_alloc = 0;
  }
}

void String::consume(uint8_t *input, int mode) {
  switch(mode) {

  case ALLOC:
	{
	  this->length = *( (uint32_t *) (input + TYPE_SIZE));
	  data = (uint8_t *) malloc(this->length);
	  cpy(this->data, input + HEADER_SIZE, this->length);
	  if_alloc = 0;
	}
	break;

  case COPY:
	{
	  this->length = *( (uint32_t *) (input + TYPE_SIZE));
	  cpy(data, input + HEADER_SIZE, this->length);
	  if_alloc = -1;
	}
	break;
	  
  case NO_COPY:
	{
	  this->length = *( (uint32_t *) (input + TYPE_SIZE));
	  data = input + HEADER_SIZE;
	  if_alloc = -1;
	}
	break;
  }

}

void String::flush(uint8_t *output) {
  *output = STRING;
  *( (uint32_t *) (output + TYPE_SIZE)) = this->length;
  cpy(output + HEADER_SIZE, this->data, this->length);
}

void String::copy_attr(String *attr, int mode) {
  this->length = attr->length;
  //this->if_alloc = attr->if_alloc;

  if (mode == NO_COPY) {
	this->data = attr->data;
  } else {
	cpy(this->data, attr->data, this->length);
  }

  
}

void String::print() {
  printf("String attr: %.*s\n", this->length, this->data);
}

GroupedAttributes::GroupedAttributes(int op_code, uint8_t *row_ptr, uint32_t num_cols) {
  this->row = row_ptr;
  this->op_code = op_code;
  this->num_cols = num_cols;
  this->expression = IDENTITY;
  this->op_code = op_code;
}

void GroupedAttributes::init() {

  uint8_t *sort_pointer = NULL;
  uint32_t len = 0;

  // Set "attributes" in the correct place to point to row
  if (op_code == 1 || op_code == 2 || op_code == 3) {
	expression = IDENTITY;

	num_attr = 1;
	num_eval_attr = 1;

	attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
	eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

	if (op_code == 2) {

	  find_plaintext_attribute(row, num_cols,
							   2, &sort_pointer, &len);
	  attributes[0] = new Integer;
	  attributes[0]->consume(sort_pointer, NO_COPY);

	  eval_attributes[0] = new Integer;
	  
	} else if (op_code == 3) {

	  find_plaintext_attribute(row, num_cols,
							   2, &sort_pointer, &len);

	  attributes[0] = new String;
	  attributes[0]->consume(sort_pointer, NO_COPY);

	  eval_attributes[0] = new String;
	}
	
	num_eval_attr = num_attr;

  } else if (op_code == 10) {
	// for Big Data Benchmark query #2
	expression = BD2;

	num_attr = 1;
	num_eval_attr = 1;

  } else if (op_code == 123456) {
	// for testing -- sum up integer columns
	expression = TEST;
	
	num_attr = 3;
	num_eval_attr = 1;

	// the attributes are #1, #3, #4

	attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
	eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);
	
	find_plaintext_attribute(row, num_cols,
							 1, &sort_pointer, &len);
	attributes[0] = new Integer;
	attributes[0]->consume(sort_pointer, NO_COPY);

	find_plaintext_attribute(row, num_cols,
							 3, &sort_pointer, &len);
	attributes[1] = new Integer;
	attributes[1]->consume(sort_pointer, NO_COPY);

	find_plaintext_attribute(row, num_cols,
							 4, &sort_pointer, &len);
	attributes[2] = new Integer;
	attributes[2]->consume(sort_pointer, NO_COPY);
  }
}

int GroupedAttributes::compare(GroupedAttributes *attr) {
  // compare the eval attributes
  
  int ret = 0;
  for (uint32_t i = 0; i < num_eval_attr; i++) {
	ret = eval_attributes[i]->compare((attr->eval_attributes)[i]);
	
	// this->eval_attributes[i]->print();
	// attr->eval_attributes[i]->print();
	// printf("ret is %d\n", eval_attributes[i]->compare((attr->eval_attributes)[i]));
	
	if (ret != 0) {	  
	  return ret;
	}
  }

  return ret;
}

void GroupedAttributes::swap(GroupedAttributes *attr) {
  // printf("Before\n");
  // this->print();
  // attr->print();
  
  swap_helper<int>(&(this->op_code), &(attr->op_code));
  swap_helper<uint8_t *>(&row, &(attr->row));
  swap_helper<uint32_t>(&num_cols, &(attr->num_cols));
  swap_helper<uint32_t>(&num_attr, &(attr->num_attr));
  swap_helper<GenericType **>(&attributes, &(attr->attributes));
  swap_helper<uint32_t>(&num_eval_attr, &(attr->num_eval_attr));
  swap_helper<GenericType **>(&eval_attributes, &(attr->eval_attributes));
  swap_helper<uint32_t>(&expression, &(attr->expression));

  // printf("After\n");
  // this->print();
  // attr->print();
}

void GroupedAttributes::print() {
  //printf("Original attributes\n");
  for (uint32_t i = 0; i < num_attr; i++) {
	attributes[i]->print();
  }

  //printf("Evaluated attributes\n");
  for (uint32_t i = 0; i < num_eval_attr; i++) {
	eval_attributes[i]->print();
  }
}

int JoinAttributes::compare(JoinAttributes *attr) {
  int ret = GroupedAttributes::compare(attr);
  if (ret != 0) {
	return ret;
  }
  
  if (ret == 0) {
	// needs to compare table id's
	int if_primary_1 = this->if_primary;
	int if_primary_2 = attr->if_primary;
	if (if_primary_1 == 0) {
	  return -1;
	} else if (if_primary_2 == 0) {
	  return 1;
	}
  }
	
  return ret;
}

void JoinAttributes::swap(JoinAttributes *attr) {
  GroupedAttributes::swap(attr);
  
  uint8_t *table_id_ = this->table_id;
  this->table_id = attr->table_id;
  attr->table_id = table_id_;

  int if_primary_ = this->if_primary;
  this->if_primary = attr->if_primary;
  attr->if_primary = if_primary_;
}

void JoinAttributes::evaluate() {
  evaluate_expr(attributes, num_attr,
				eval_attributes, num_eval_attr,
				expression, JOIN);
}

void JoinAttributes::print() {
  GroupedAttributes::print();
}

void SortAttributes::evaluate() {
  evaluate_expr(attributes, num_attr,
				eval_attributes, num_eval_attr,
				expression, SORT);
}

int SortAttributes::compare(SortAttributes *attr) {
  int ret = GroupedAttributes::compare(attr);
  return ret;
}

void ProjectAttributes::evaluate() {
  evaluate_expr(attributes, num_attr,
				eval_attributes, num_eval_attr,
				expression, PROJECT);
}


int JoinRecord::compare(JoinRecord *rec) {
  int ret = this->join_attributes->compare(rec->join_attributes);
  return ret;
}

void JoinRecord::swap(JoinRecord *rec) {
  Record::swap(rec);
  join_attributes->swap(rec->join_attributes);
}

void JoinRecord::compare_and_swap(JoinRecord *rec) {
  if (this->compare(rec) == 1) {
	this->swap(rec);
  }
}


// sets data directly
void JoinRecord::consume_encrypted_row(uint8_t *enc_row) {
  decrypt(enc_row, enc_size(JOIN_ROW_UPPER_BOUND), row);
  this->num_cols = *( (uint32_t *) (row + TABLE_ID_SIZE));
}

void JoinRecord::set_join_attributes(int op_code) {
  join_attributes = new JoinAttributes(op_code, row + TABLE_ID_SIZE + 4, num_cols);
  join_attributes->set_table_id(row);
  join_attributes->if_primary = is_table_primary(row);
}


int SortRecord::compare(SortRecord *rec) {
  int ret = this->sort_attributes->compare(rec->sort_attributes);
  return ret;
}

void SortRecord::swap(SortRecord *rec) {
  Record::swap(rec);
  sort_attributes->swap(rec->sort_attributes);
}

void SortRecord::compare_and_swap(SortRecord *rec) {
  if (this->compare(rec) == 1) {
	printf("Swapping\n");
	this->swap(rec);
  }
}
