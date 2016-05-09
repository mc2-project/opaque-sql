#include "InternalTypes.h"

GenericType *create_attr(uint8_t *attr) {
  uint8_t type = *attr;
  switch (type) {

  case INT: return new Integer; break;
  case STRING: return new String; break;
  case FLOAT: return new Float; break;
  case DATE: return new Date; break;
  case URL_TYPE: return new URL; break;
  case C_CODE: return new CountryCode; break;
  case L_CODE: return new LanguageCode; break;
  case LONG: assert(false); break;
  case IP_TYPE: return new IP; break;
  case USER_AGENT_TYPE: return new UserAgent; break;
  case SEARCH_WORD_TYPE: return new SearchWord; break;
  case DUMMY_INT: return new Dummy(DUMMY_INT); break;
  case DUMMY_FLOAT: return new Dummy(DUMMY_FLOAT); break;
  case DUMMY_STRING: return new Dummy(DUMMY_STRING); break;

  default:
    printf("create_attr: Unknown type %d\n", type);
    assert(false);

  }
}

/*** DUMMY ***/
void Dummy::print() {
  printf("Dummy with type %d\n", real_type_);
}

/*** INTEGER ***/
Integer::Integer() {
  value = 0;
  type_ = INT;
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
	
void Integer::consume(uint8_t *input, int mode) {
  uint8_t type = *input;
  if (type == INT) {
    this->value = *( (uint32_t *) (input + HEADER_SIZE));
  } else {
    printf("Integer::consume: Unexpected type %d\n", type);
    assert(false);
  }
}
  
void Integer::flush(uint8_t *output) {
  // write back type, size, and actual output
  *output = INT;
  *( (uint32_t *) (output + TYPE_SIZE)) = 4;
  *( (uint32_t *) (output + HEADER_SIZE)) = value;
}

void Integer::copy_attr(Integer *v) {
  this->value = v->value;
}

void Integer::print() {
  printf("Integer attr: %u\n", this->value);
}

void Integer::sum(GenericType *v) {
  if (v->type_ == INT) {
    this->value += dynamic_cast<Integer *>(v)->value;
  } else {
    printf("Integer::sum: Unexpected type %d\n", v->type_);
    assert(false);
  }
}

/*** INTEGER ***/

/*** STRING ***/
String::String() {
  data = NULL;
  length = 0;
  if_alloc = -1;
  type_ = STRING;
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
	
  if (buffer != NULL) {
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

  default:
    printf("String::consume: Unknown mode %d\n", mode);
    assert(false);
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

void String::reset() {
  if (if_alloc == 0) {
    free(data);
  }
	
  data = NULL;
  if_alloc = -1;
  length = 0;
}

void String::print() {
  printf("String attr: %.*s\n", this->length, this->data);
}

/*** STRING ***/

/*** Float ***/

Float::Float() {
  value = 0;
  type_ = FLOAT;
}

int Float::compare(GenericType *v) {
  Float *float_v = dynamic_cast<Float *>(v);
  
  if (this->value > float_v->value) {
	return 1;
  } else if (this->value < float_v->value) {
	return -1;
  }

  return 0;
}

void Float::swap(GenericType *v) {

  Float *float_v = dynamic_cast<Float *>(v);

  float temp = this->value;
  this->value = float_v->value;
  float_v->value = temp;
}

void Float::consume(uint8_t *input, int mode) {
  uint8_t type = *input;
  if (type == FLOAT) {
	this->value = *( (float *) (input + HEADER_SIZE));
  } else {
    printf("Float::consume: Unexpected type %d\n", type);
    assert(false);
  }
  
}

void Float::flush(uint8_t *output) {
  *output = FLOAT;
  *( (uint32_t *) (output + TYPE_SIZE)) = 4;
  *( (float *) (output + HEADER_SIZE)) = value;
}

void Float::copy_attr(Float *attr) {
  this->value = attr->value;
}

void Float::sum(GenericType *v) {
  if (v->type_ == FLOAT) {
    this->value += dynamic_cast<Float *>(v)->value;
  } else {
    printf("Float::sum: Unexpected type %d\n", v->type_);
    assert(false);
  }
}

void Float::print() {
  printf("Float attr: %f\n", this->value);  
}

/*** Float ***/

/*** Date ***/

Date::Date() {
  date = 0;
  type_ = DATE;
}

Date::Date(uint64_t date) {
  this->date = date;
}

int Date::compare(GenericType *v) {
  Date *date_v = dynamic_cast<Date *>(v);

  if (date_v == NULL) {
    printf("Date::compare -- date_v is not type Date, instead %u\n", v->type_);
    assert(false);
  } else {
    if (this->date > date_v->date) {
      return 1;
    } else if (this->date < date_v->date) {
      return -1;
    }
  }

  return 0;
}

void Date::swap(GenericType *v) {

  Date *date_v = dynamic_cast<Date *>(v);

  uint64_t temp = this->date;
  this->date = date_v->date;
  date_v->date = temp;
}

void Date::consume(uint8_t *input, int mode) {
  uint8_t type = *input;
  if (type == DATE) {
	this->date = *( (float *) (input + HEADER_SIZE));
  } else {
    printf("Date::consume: Unexpected type %d\n", type);
    assert(false);
  }
  
}

void Date::flush(uint8_t *output) {
  *output = DATE;
  *( (uint32_t *) (output + TYPE_SIZE)) = 4;
  *( (uint64_t *) (output + HEADER_SIZE)) = date;
}

void Date::copy_attr(Date *attr, int mode) {
  this->date = attr->date;
}

void Date::print() {
  printf("Date attr: %lu\n", this->date);
}

/*** Date ***/


/*** URL ***/

URL::URL() {
  type_ = URL_TYPE;
}

void URL::print() {
  printf("URL attr: %.*s\n", this->length, this->data);
}

/*** URL ***/


/*** CountryCode ***/

CountryCode::CountryCode() {
  type_ = C_CODE;
}

void CountryCode::print() {
  printf("CountryCode attr: %.*s\n", this->length, this->data);
}

/*** CountryCode ***/



/*** LanguageCode ***/

LanguageCode::LanguageCode() {
  type_ = L_CODE;
}

void LanguageCode::print() {
  printf("LangaugeCode attr: %.*s\n", this->length, this->data);
}

/*** LanguageCode ***/


/*** IP ***/

IP::IP() {
  type_ = IP_TYPE;
}

void IP::print() {
  printf("IP attr: %.*s\n", this->length, this->data);
}

/*** IP ***/


/*** User Agent ***/

UserAgent::UserAgent() {
  type_ = USER_AGENT_TYPE;
}

void UserAgent::print() {
  printf("UserAgent attr: %.*s\n", this->length, this->data);
}

/*** User Agent ***/


/*** Search word ***/

SearchWord::SearchWord() {
  type_ = SEARCH_WORD_TYPE;
}

void SearchWord::print() {
  printf("SearchWord attr: %.*s\n", this->length, this->data);
}

/*** User Agent ***/


/*** GROUPED ATTRIBUTES ***/
GroupedAttributes::GroupedAttributes(int op_code, uint8_t *row_ptr, uint32_t num_cols) {
  this->row = row_ptr;
  this->op_code = op_code;
  this->num_cols = num_cols;
  this->expression = IDENTITY;
  this->op_code = op_code;
}

int GroupedAttributes::compare(GroupedAttributes *attr) {
  // compare the eval attributes
  
  int ret = 0;
  for (uint32_t i = 0; i < num_eval_attr; i++) {
	ret = eval_attributes[i]->compare((attr->eval_attributes)[i]);
	
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
  printf("Original attributes:  ");
  for (uint32_t i = 0; i < num_attr; i++) {
	attributes[i]->print();
  }

  printf("Evaluated attributes:  ");
  for (uint32_t i = 0; i < num_eval_attr; i++) {
	eval_attributes[i]->print();
  }
}

/*** GROUPED ATTRIBUTES ***/


/*** PROJECT ATTRIBUTES ***/

void ProjectAttributes::init() {
  uint8_t *sort_pointer = NULL;
  uint32_t len = 0;

  // Set "attributes" in the correct place to point to row
  // For JoinAttributes, can look at the different columns for primary & foreign key tables
  if (op_code == OP_BD2) {
    // for Big Data Benchmark query #2
    expression = BD2;
    
    num_attr = 2;
    num_eval_attr = 2;

    attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
    eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

    find_plaintext_attribute(row, num_cols,
                             1, &sort_pointer, &len);
    attributes[0] = create_attr(sort_pointer);
    eval_attributes[0] = create_attr(sort_pointer);
    attributes[0]->consume(sort_pointer, NO_COPY);

    find_plaintext_attribute(row, num_cols,
                             2, &sort_pointer, &len);
    attributes[1] = create_attr(sort_pointer);
    eval_attributes[1] = create_attr(sort_pointer);
    attributes[1]->consume(sort_pointer, NO_COPY);

    num_eval_attr = num_attr;

  } else if (op_code == OP_PROJECT_PAGERANK_WEIGHT_RANK) {

    expression = PR_WEIGHT_RANK;

    num_attr = 3;
    num_eval_attr = 2;

    attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
    eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

    for (uint32_t i = 0; i < num_attr; i++) {
      find_plaintext_attribute(row, num_cols,
                   2 + i, &sort_pointer, &len);
      attributes[i] = create_attr(sort_pointer);
      attributes[i]->consume(sort_pointer, NO_COPY);
    }

    eval_attributes[0] = new Integer;
    eval_attributes[1] = new Float;

  } else if (op_code == OP_PROJECT_PAGERANK_APPLY_INCOMING_RANK) {

    expression = PR_APPLY_INCOMING_RANK;

    num_attr = 2;
    num_eval_attr = 2;

    attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
    eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

    find_plaintext_attribute(row, num_cols,
			     1, &sort_pointer, &len);

    attributes[0] = create_attr(sort_pointer);
    attributes[0]->consume(sort_pointer, NO_COPY);

    eval_attributes[0] = create_attr(sort_pointer);
    eval_attributes[0]->reset();

    find_plaintext_attribute(row, num_cols,
                             2, &sort_pointer, &len);
    attributes[1] = create_attr(sort_pointer);
    attributes[1]->consume(sort_pointer, NO_COPY);

    eval_attributes[1] = create_attr(sort_pointer);
    eval_attributes[1]->reset();

  } else {
    printf("ProjectAttributes::init: Unknown opcode %d\n", op_code);
    assert(false);
  }

}

void ProjectAttributes::re_init(uint8_t *new_row_ptr) {
  uint8_t *sort_pointer = NULL;
  uint32_t len = 0;

  if (this->op_code == OP_BD2) {
    attributes[0]->reset();
    find_plaintext_attribute(new_row_ptr, num_cols, 1, &sort_pointer, &len);
	attributes[0]->consume(sort_pointer, NO_COPY);

    attributes[1]->reset();
    find_plaintext_attribute(new_row_ptr, num_cols, 2, &sort_pointer, &len);
    attributes[1]->consume(sort_pointer, NO_COPY);

  } else if (this->op_code == OP_PROJECT_PAGERANK_WEIGHT_RANK) {
    
    find_plaintext_attribute(row, num_cols,
                             2, &sort_pointer, &len);
    attributes[0]->consume(sort_pointer, NO_COPY);

    find_plaintext_attribute(row, num_cols,
                             3, &sort_pointer, &len);
    attributes[1]->consume(sort_pointer, NO_COPY);

    find_plaintext_attribute(row, num_cols,
                             4, &sort_pointer, &len);
    attributes[2]->consume(sort_pointer, NO_COPY);


    eval_attributes[0]->reset();
    eval_attributes[1]->reset();
    
  } else if (this->op_code == OP_PROJECT_PAGERANK_APPLY_INCOMING_RANK) {

    find_plaintext_attribute(row, num_cols,
			     1, &sort_pointer, &len);
    attributes[0]->consume(sort_pointer, NO_COPY);
    eval_attributes[0]->reset();

    find_plaintext_attribute(row, num_cols,
                             2, &sort_pointer, &len);
    attributes[1]->consume(sort_pointer, NO_COPY);
    eval_attributes[1]->reset();

  } else {
    printf("ProjectAttributes::re_init: Unknown opcode %d\n", op_code);
    assert(false);
  }
}


void ProjectAttributes::evaluate() {
  evaluate_expr(attributes, num_attr,
				eval_attributes, num_eval_attr,
				expression, PROJECT);
}


/*** PROJECT ATTRIBUTES ***/


/*** JOIN ATTRIBUTES ***/

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

void JoinAttributes::init() {

  uint8_t *sort_pointer = NULL;
  uint32_t len = 0;

  // Set "attributes" in the correct place to point to row
  // For JoinAttributes, can look at the different columns for primary & foreign key tables
  if (op_code == OP_JOIN_COL1) {
    expression = IDENTITY;

    num_attr = 1;
    num_eval_attr = 1;

    attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
    eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

    find_plaintext_attribute(row, num_cols,
                             1, &sort_pointer, &len);

    attributes[0] = create_attr(sort_pointer);
    eval_attributes[0] = create_attr(sort_pointer);
    attributes[0]->consume(sort_pointer, NO_COPY);

    num_eval_attr = num_attr;

  } else if (op_code == OP_JOIN_COL2) {
    expression = IDENTITY;

    num_attr = 1;
    num_eval_attr = 1;

    attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
    eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

    find_plaintext_attribute(row, num_cols,
                             2, &sort_pointer, &len);

    attributes[0] = create_attr(sort_pointer);
    eval_attributes[0] = create_attr(sort_pointer);
    attributes[0]->consume(sort_pointer, NO_COPY);

    num_eval_attr = num_attr;

  } else if (op_code == OP_BD2) {
    // for Big Data Benchmark query #2
    expression = BD2;

    num_attr = 1;
    num_eval_attr = 1;

  } else {
    printf("JoinAttributes::init: Unknown opcode %d\n", op_code);
    assert(false);
  }
}

void JoinAttributes::re_init(uint8_t *new_row_ptr) {
  uint8_t *sort_pointer = NULL;
  uint32_t len = 0;

  // Set "attributes" in the correct place to point to row
  // For JoinAttributes, can look at the different columns for primary & foreign key tables
  if (op_code == OP_JOIN_COL1) {
    expression = IDENTITY;

    find_plaintext_attribute(row, num_cols,
                             1, &sort_pointer, &len);

    attributes[0]->reset();
    eval_attributes[0]->reset();
    attributes[0]->consume(sort_pointer, NO_COPY);

  } else if (op_code == OP_JOIN_COL2) {
    expression = IDENTITY;

    find_plaintext_attribute(row, num_cols,
                             2, &sort_pointer, &len);

    attributes[0]->reset();
    eval_attributes[0]->reset();
    attributes[0]->consume(sort_pointer, NO_COPY);

  } else if (op_code == OP_BD2) {
	// for Big Data Benchmark query #2
	expression = BD2;

	num_attr = 1;
	num_eval_attr = 1;

  } else {
    printf("JoinAttributes::re_init: Unknown opcode %d\n", op_code);
    assert(false);
  }
}

void JoinAttributes::evaluate() {
  evaluate_expr(attributes, num_attr,
				eval_attributes, num_eval_attr,
				expression, JOIN);
}

void JoinAttributes::print() {
  GroupedAttributes::print();
}

/*** JOIN ATTRIBUTES ***/


/*** SORT ATTRIBUTES ***/

void SortAttributes::init() {

  uint8_t *sort_pointer = NULL;
  uint32_t len = 0;
  
  if (op_code == OP_SORT_COL1 || op_code == OP_SORT_COL2) {
	expression = IDENTITY;

	num_attr = 1;
	num_eval_attr = 1;

	attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
	eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

    uint32_t sort_col = 0;
    switch (op_code) {
    case OP_SORT_COL1: sort_col = 1; break;
    case OP_SORT_COL2: sort_col = 2; break;
    default:
      printf("SortAttributes::init: Unknown opcode %d\n", op_code);
      assert(false);
    }
	find_plaintext_attribute(row, num_cols,
                             sort_col, &sort_pointer, &len);
    attributes[0] = create_attr(sort_pointer);
    eval_attributes[0] = create_attr(sort_pointer);
	attributes[0]->consume(sort_pointer, NO_COPY);
	
    num_eval_attr = num_attr;

  } else if (op_code == OP_SORT_COL4_IS_DUMMY_COL2) {
	// this sort is the last step of aggregation

	num_attr = 2;
	num_eval_attr = 2;

	// first, sort based on the aggregation attribute's type
	// if not dummy, sort based on the agg sort attribute

    attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
    eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

	find_plaintext_attribute(row, num_cols,
							 4, &sort_pointer, &len);

	attributes[0] = create_attr(sort_pointer);
	eval_attributes[0] = create_attr(sort_pointer);
	
    find_plaintext_attribute(row, num_cols,
                             2, &sort_pointer, &len);
	
    attributes[1] = create_attr(sort_pointer);
	eval_attributes[1] = create_attr(sort_pointer);
    attributes[1]->consume(sort_pointer, NO_COPY);
  } else if (op_code == OP_SORT_COL3_IS_DUMMY_COL1) {
    // this sort is the last step of aggregation

    num_attr = 2;
    num_eval_attr = 2;

    // first, sort based on the aggregation attribute's type
    // if not dummy, sort based on the agg sort attribute

    attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
    eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

    find_plaintext_attribute(row, num_cols,
                             3, &sort_pointer, &len);

    attributes[0] = create_attr(sort_pointer);
    eval_attributes[0] = create_attr(sort_pointer);

    find_plaintext_attribute(row, num_cols,
                             1, &sort_pointer, &len);

    attributes[1] = create_attr(sort_pointer);
    eval_attributes[1] = create_attr(sort_pointer);
    attributes[1]->consume(sort_pointer, NO_COPY);
  } else {
    printf("SortAttributes::init: unknown opcode %d\n", op_code);
    assert(false);
  }

}

void SortAttributes::re_init(uint8_t *new_row_ptr) {
  uint8_t *sort_pointer = NULL;
  uint32_t len = 0;

  if (op_code == OP_SORT_COL1 || op_code == OP_SORT_COL2) {
	
    uint32_t sort_col = 0;
    switch (op_code) {
    case OP_SORT_COL1: sort_col = 1; break;
    case OP_SORT_COL2: sort_col = 2; break;
    default:
      printf("SortAttributes::init: Unknown opcode %d\n", op_code);
      assert(false);
    }
    	find_plaintext_attribute(row, num_cols,
                             sort_col, &sort_pointer, &len);
    attributes[0]->reset();
    eval_attributes[0]->reset();
    attributes[0]->consume(sort_pointer, NO_COPY);

	
  } else if (op_code == OP_SORT_COL4_IS_DUMMY_COL2) {
	// this sort is the last step of aggregation

	// first, sort based on the aggregation attribute's type
	// if not dummy, sort based on the agg sort attribute

	attributes[0]->reset();

	find_plaintext_attribute(row, num_cols,
							 4, &sort_pointer, &len);

	delete attributes[0];
	delete eval_attributes[0];

	attributes[0] = create_attr(sort_pointer);
	eval_attributes[0] = create_attr(sort_pointer);
	
    find_plaintext_attribute(row, num_cols,
                             2, &sort_pointer, &len);
    attributes[1]->reset();
	eval_attributes[1]->reset();
    attributes[1]->consume(sort_pointer, NO_COPY);
  } else if (op_code == OP_SORT_COL3_IS_DUMMY_COL1) {
    // this sort is the last step of aggregation

    // first, sort based on the aggregation attribute's type
    // if not dummy, sort based on the agg sort attribute

    attributes[0]->reset();

    find_plaintext_attribute(row, num_cols,
                             3, &sort_pointer, &len);

    delete attributes[0];
    delete eval_attributes[0];

    attributes[0] = create_attr(sort_pointer);
    eval_attributes[0] = create_attr(sort_pointer);

    find_plaintext_attribute(row, num_cols,
                             1, &sort_pointer, &len);
    attributes[1]->reset();
    eval_attributes[1]->reset();
    attributes[1]->consume(sort_pointer, NO_COPY);
  } else {
    printf("SortAttributes::re_init: unknown opcode %d\n", op_code);
    assert(false);
  }
  
}

void SortAttributes::evaluate() {
  evaluate_expr(attributes, num_attr,
				eval_attributes, num_eval_attr,
				expression, SORT);
}

int SortAttributes::compare(SortAttributes *attr) {
  if (op_code == OP_SORT_COL4_IS_DUMMY_COL2 || op_code == OP_SORT_COL3_IS_DUMMY_COL1) {

    int ret = 0;
    for (uint32_t i = 0; i < num_eval_attr; i++) {
      if (i == 0) {
	uint8_t type1 = this->eval_attributes[0]->type_;
	uint8_t type2 = attr->eval_attributes[0]->type_;

	if (type1 == DUMMY) {
	  return -1;
	} else if (type2 == DUMMY) {
	  return 1;
	}

      } else {
	ret = eval_attributes[i]->compare((attr->eval_attributes)[i]);
		
	if (ret != 0) {	  
	  return ret;
	}
      }
    }

  } else if (op_code == OP_SORT_COL1 || op_code == OP_SORT_COL2 || op_code == OP_SORT_INTEGERS_TEST) {
    int ret = GroupedAttributes::compare(attr);
    return ret;
  } else {
    printf("SortAttributes::compare: Unknown opcode %d\n", op_code);
    assert(false);
  }
}

/*** SORT ATTRIBUTES ***/

/*** AGG SORT ATTR ***/

void AggSortAttributes::init() {
  uint8_t *sort_pointer = NULL;
  uint32_t len = 0;

  // Set "attributes" in the correct place to point to row

  // printf("init called, op_code is %u\n", op_code);
  if (op_code == OP_GROUPBY_COL2_SUM_COL3_STEP1 || op_code == OP_GROUPBY_COL2_SUM_COL3_STEP2) {
    expression = IDENTITY;

    num_attr = 1;
    num_eval_attr = 1;

    attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
    eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

    find_plaintext_attribute(row, num_cols,
			     2, &sort_pointer, &len);

    attributes[0] = create_attr(sort_pointer);
    eval_attributes[0] = create_attr(sort_pointer);
    attributes[0]->consume(sort_pointer, NO_COPY);

    num_eval_attr = num_attr;

  } else if (op_code == OP_GROUPBY_COL1_SUM_COL2_STEP1 || op_code == OP_GROUPBY_COL1_SUM_COL2_STEP2) {
    expression = IDENTITY;

    num_attr = 1;
    num_eval_attr = 1;

    attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
    eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

    find_plaintext_attribute(row, num_cols,
                             1, &sort_pointer, &len);

    attributes[0] = create_attr(sort_pointer);
    eval_attributes[0] = create_attr(sort_pointer);
    attributes[0]->consume(sort_pointer, NO_COPY);

    num_eval_attr = num_attr;
  } else if (op_code == OP_GROUPBY_COL1_AVG_COL2_SUM_COL3_STEP1 ||
             op_code == OP_GROUPBY_COL1_AVG_COL2_SUM_COL3_STEP2) {
    expression = IDENTITY;

    num_attr = 1;
    num_eval_attr = 1;

    attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
    eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

    find_plaintext_attribute(row, num_cols,
                             1, &sort_pointer, &len);

    attributes[0] = create_attr(sort_pointer);
    eval_attributes[0] = create_attr(sort_pointer);
    attributes[0]->consume(sort_pointer, NO_COPY);

    num_eval_attr = num_attr;
  } else {
    printf("AggSortAttributes::init: Unknown opcode %d\n", op_code);
    assert(false);
  }
}

void AggSortAttributes::re_init(uint8_t *new_row_ptr) {

  uint8_t *sort_pointer = NULL;
  uint32_t len = 0;

  if (this->op_code == OP_GROUPBY_COL2_SUM_COL3_STEP1
      || this->op_code == OP_GROUPBY_COL2_SUM_COL3_STEP2) {
    attributes[0]->reset();

    find_plaintext_attribute(new_row_ptr, num_cols,
			     2, &sort_pointer, &len);

    attributes[0]->consume(sort_pointer, NO_COPY);
    // printf("Re-initialize, new attributes is ");
    // attributes[0]->print();
  } else if (this->op_code == OP_GROUPBY_COL1_SUM_COL2_STEP1
      || this->op_code == OP_GROUPBY_COL1_SUM_COL2_STEP2) {
    attributes[0]->reset();

    find_plaintext_attribute(new_row_ptr, num_cols,
                             1, &sort_pointer, &len);

    attributes[0]->consume(sort_pointer, NO_COPY);
  } else if (op_code == OP_GROUPBY_COL1_AVG_COL2_SUM_COL3_STEP1 ||
             op_code == OP_GROUPBY_COL1_AVG_COL2_SUM_COL3_STEP2) {
    attributes[0]->reset();

    find_plaintext_attribute(new_row_ptr, num_cols,
                             1, &sort_pointer, &len);

    attributes[0]->consume(sort_pointer, NO_COPY);
  } else {
    printf("AggSortAttributes::re_init: Unknown opcode %d\n", this->op_code);
    assert(false);
  }
}

void AggSortAttributes::evaluate() {
  evaluate_expr(attributes, num_attr,
		eval_attributes, num_eval_attr,
		expression, AGG);
}

int AggSortAttributes::compare(AggSortAttributes *attr) {
  int ret = GroupedAttributes::compare(attr);
  return ret;
}

/*** AGG SORT ATTR ***/


/*** AGG AGG ATTR ***/

void AggAggAttributes::init() {
  uint8_t *sort_pointer = NULL;
  uint32_t len = 0;

  // Set "attributes" in the correct place to point to row

  // printf("init called, op_code is %u\n", op_code);
  if (op_code == OP_GROUPBY_COL2_SUM_COL3_STEP1 || op_code == OP_GROUPBY_COL2_SUM_COL3_STEP2) {
    expression = IDENTITY;

    num_attr = 1;
    num_eval_attr = 1;

    attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
    eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

    find_plaintext_attribute(row, num_cols,
			     3, &sort_pointer, &len);

    attributes[0] = create_attr(sort_pointer);
    eval_attributes[0] = create_attr(sort_pointer);

    attributes[0]->consume(sort_pointer, NO_COPY);

    num_eval_attr = num_attr;

  } else if (op_code == OP_GROUPBY_COL1_SUM_COL2_STEP1 || op_code == OP_GROUPBY_COL1_SUM_COL2_STEP2) {
    expression = IDENTITY;

    num_attr = 1;
    num_eval_attr = 1;

    attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
    eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

    find_plaintext_attribute(row, num_cols,
                 2, &sort_pointer, &len);

    attributes[0] = create_attr(sort_pointer);
    eval_attributes[0] = create_attr(sort_pointer);

    attributes[0]->consume(sort_pointer, NO_COPY);

    num_eval_attr = num_attr;

  } else if (op_code == OP_GROUPBY_COL1_AVG_COL2_SUM_COL3_STEP1 || 
	     op_code == OP_GROUPBY_COL1_AVG_COL2_SUM_COL3_STEP2) {

    expression = IDENTITY;

    num_attr = 2;
    num_eval_attr = 2;

    attributes = (GenericType **) malloc(sizeof(GenericType *) * num_attr);
    eval_attributes = (GenericType **) malloc(sizeof(GenericType *) * num_eval_attr);

    find_plaintext_attribute(row, num_cols,
                 2, &sort_pointer, &len);

    attributes[0] = create_attr(sort_pointer);
    eval_attributes[0] = create_attr(sort_pointer);

    attributes[0]->consume(sort_pointer, NO_COPY);

    find_plaintext_attribute(row, num_cols,
                 3, &sort_pointer, &len);

    attributes[1] = create_attr(sort_pointer);
    eval_attributes[1] = create_attr(sort_pointer);

    attributes[1]->consume(sort_pointer, NO_COPY);

    num_eval_attr = num_attr;
    
  } else {
    printf("AggAggAttributes::init: Unknown opcode %d\n", op_code);
    assert(false);
  }
}

void AggAggAttributes::re_init(uint8_t *new_row_ptr) {

  uint8_t *sort_pointer = NULL;
  uint32_t len = 0;

  if (this->op_code == OP_GROUPBY_COL2_SUM_COL3_STEP1 || this->op_code == OP_GROUPBY_COL2_SUM_COL3_STEP2) {
    attributes[0]->reset();
    find_plaintext_attribute(new_row_ptr, num_cols,
			     3, &sort_pointer, &len);
    attributes[0]->consume(sort_pointer, NO_COPY);
  } else if (this->op_code == OP_GROUPBY_COL1_SUM_COL2_STEP1 || this->op_code == OP_GROUPBY_COL1_SUM_COL2_STEP2) {
    attributes[0]->reset();
    find_plaintext_attribute(new_row_ptr, num_cols,
                 2, &sort_pointer, &len);
    attributes[0]->consume(sort_pointer, NO_COPY);

  } else if (op_code == OP_GROUPBY_COL1_AVG_COL2_SUM_COL3_STEP1 || 
	     op_code == OP_GROUPBY_COL1_AVG_COL2_SUM_COL3_STEP2) {
    find_plaintext_attribute(new_row_ptr, num_cols,
                 2, &sort_pointer, &len);

    attributes[0]->consume(sort_pointer, NO_COPY);

    find_plaintext_attribute(new_row_ptr, num_cols,
                 3, &sort_pointer, &len);

    attributes[1]->consume(sort_pointer, NO_COPY);

    num_eval_attr = num_attr;
  } else {
    printf("AggAggAttributes::re_init: Unknown opcode %d\n", this->op_code);
    assert(false);
  }

}

void AggAggAttributes::evaluate() {
  evaluate_expr(attributes, num_attr,
		eval_attributes, num_eval_attr,
		expression, AGG_AGG);
}

int AggAggAttributes::compare(AggAggAttributes *attr) {
  int ret = GroupedAttributes::compare(attr);
  return ret;
}

/*** AGG AGG ATTR ***/


/*** RECORD ***/

// given a row with a set of encrypted attributes
// assume that the row_ptr is set correctly
// do not write into 
uint32_t Record::consume_all_encrypted_attributes(uint8_t *input_row) {
  this->num_cols = *( (uint32_t *) (input_row));
  uint8_t *input_row_ptr = input_row + 4;
  uint32_t enc_len = 0;
  uint32_t total_len = 4;

  for (uint32_t i = 0; i < this->num_cols; i++) {
	// enc_len = *( (uint32_t *) (input_row_ptr));
	// input_row_ptr += 4;
	// decrypt(input_row_ptr, enc_len, row_ptr);
	
	// input_row_ptr += enc_len;
	// row_ptr += dec_size(enc_len);
	// total_len += enc_len;

	decrypt_attribute(&input_row_ptr, &row_ptr);
  }

  return (input_row_ptr - input_row);
}

void Record::consume_encrypted_attribute(uint8_t **enc_value_ptr) {
  decrypt_attribute(enc_value_ptr, &row_ptr);
  num_cols += 1;
}

/*** RECORD ***/


/*** PROJECT RECORD  ***/

void ProjectRecord::clear() {
  this->num_cols = 0;
  this->row_ptr = this->row;
}

void ProjectRecord::set_project_attributes(int op_code) {
  
  if (project_attributes == NULL) {
	project_attributes = new ProjectAttributes(op_code, row, num_cols);
	project_attributes->init();
	project_attributes->evaluate();
  } else {
    project_attributes->re_init(this->row);
	project_attributes->evaluate();
  }

}

// only the attributes!
uint32_t ProjectRecord::flush_encrypt_eval_attributes(uint8_t *output) {
  uint8_t *output_ptr = output;
  uint32_t value_len = 0;
  
  ProjectAttributes *attrs = this->project_attributes;

  *( (uint32_t *) (output_ptr)) = attrs->num_eval_attr;
  output_ptr += 4;

  uint8_t temp[ROW_UPPER_BOUND];
  uint8_t *temp_ptr = temp;

  for (uint32_t i = 0; i < attrs->num_eval_attr; i++) {

    temp_ptr = temp;
    attrs->eval_attributes[i]->flush(temp);
    encrypt_attribute(&temp_ptr, &output_ptr);
	
    // value_len = *( (uint32_t *) (temp + TYPE_SIZE));
    // *( (uint32_t *) output_ptr ) = enc_size(value_len + HEADER_SIZE);
    
    // encrypt(temp, value_len + HEADER_SIZE, output_ptr + 4);
    
    // output_ptr += enc_size(value_len + HEADER_SIZE) + 4;
  }

  return (output_ptr - output);
}


/*** PROJECT RECORD  ***/

/*** JOIN RECORD  ***/

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

void JoinRecord::consume_plaintext_row(uint8_t *plain_row) {
  memcpy(row, plain_row, JOIN_ROW_UPPER_BOUND);
  this->num_cols = *( (uint32_t *) (row + TABLE_ID_SIZE));
}

void JoinRecord::set_join_attributes(int op_code) {
  if (join_attributes == NULL) {
	join_attributes = new JoinAttributes(op_code, row + TABLE_ID_SIZE + 4, num_cols);
	join_attributes->set_table_id(row);
	join_attributes->if_primary = is_table_primary(row);
	join_attributes->init();
	join_attributes->evaluate();
  } else {
	join_attributes->re_init(this->row + TABLE_ID_SIZE + 4);
	join_attributes->set_table_id(row);
	join_attributes->if_primary = is_table_primary(row);
	join_attributes->evaluate();
  }
}

void JoinRecord::reset() {
  this->num_cols = 0;
  this->row_ptr = this->row;
}

/*** JOIN RECORD  ***/


/*** SORT RECORD  ***/
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
    //printf("Swapping\n");
	this->swap(rec);
  }
}

void SortRecord::reset() {
  this->num_cols = 0;
  this->row_ptr = this->row + 4;
}

void SortRecord::set_sort_attributes(int op_code) {
  if (sort_attributes == NULL) {
    sort_attributes = new SortAttributes(op_code, row + 4, num_cols);
    sort_attributes->init();
    sort_attributes->evaluate();
  } else {
    sort_attributes->re_init(this->row + 4);
    sort_attributes->evaluate();
  }
}

void SortRecord::consume_encrypted_attribute(uint8_t **enc_value_ptr) {
  decrypt_attribute(enc_value_ptr, &row_ptr);
  num_cols += 1;
  *( (uint32_t *) this->row) = num_cols;
}

/*** SORT RECORD  ***/

/*** AGG SORT RECORD  ***/
void AggRecord::copy(AggRecord *rec, int mode) {
  if (mode == COPY) {
    this->num_cols = rec->num_cols;
	
    //printf("aggrecord copy, row=%p, rec=%p, rec->row=%p\n", this->row, rec, rec->row);
    cpy(this->row, rec->row, AGG_UPPER_BOUND);
    //printf("After cpy\n");

    // print_row("", this->row + 4 + 4);
    // printf("After print_row\n");
    reset_row_ptr();
    // printf("aggrecord set attr1 %p, %p\n",
    //        this->agg_sort_attributes,
    //        rec->agg_sort_attributes);
    this->set_agg_sort_attributes(rec->agg_sort_attributes->op_code);
    //printf("aggrecord set attr2\n");
  }
}

int AggRecord::compare(AggRecord *rec) {
  int ret = this->agg_sort_attributes->compare(rec->agg_sort_attributes);
  return ret;
}

void AggRecord::reset() {
  row_ptr = row + 4 + 4;
  write_dummy(row, AGG_UPPER_BOUND);
}

void AggRecord::print() {
  printf("Aggregate record, number of columns: %u\n", this->num_cols);
  // printf("===============\n");
  uint8_t *value_ptr = row + 4 + 4 + 4;
  for (uint32_t i = 0; i < this->num_cols; i++) {
	print_attribute("", value_ptr);
	value_ptr += *( (uint32_t *) (value_ptr + TYPE_SIZE)) + HEADER_SIZE;
  }

  printf("Agg attributes:\n");
  agg_sort_attributes->print();
  //printf("===============\n");
}

void AggRecord::consume_enc_agg_record(uint8_t *input, uint32_t enc_len) {
  decrypt(input, enc_len, this->row);
  this->num_cols = *( (uint32_t *) (this->row + 4 + 4));
  this->reset_row_ptr();
}

void AggRecord::set_agg_sort_attributes(int op_code) {
  //printf("agg_sort_attributes is %p, num_cols is %u\n", agg_sort_attributes, num_cols);
  if (agg_sort_attributes == NULL) {
    agg_sort_attributes = new AggSortAttributes(op_code, row + 4 + 4 + 4, num_cols);
    agg_sort_attributes->init();
    agg_sort_attributes->evaluate();
  }
  if (agg_sort_attributes != NULL) {
    agg_sort_attributes->re_init(this->row + 4 + 4 + 4);
    agg_sort_attributes->evaluate();
  }

  if (agg_agg_attributes == NULL) {
    agg_agg_attributes = new AggAggAttributes(op_code, row + 4 + 4 + 4, num_cols);
    agg_agg_attributes->init();
    agg_agg_attributes->evaluate();
  }
  if (agg_agg_attributes != NULL) {
    agg_agg_attributes->re_init(this->row + 4 + 4 + 4);
    agg_agg_attributes->evaluate();
  }
}

uint32_t AggRecord::consume_all_encrypted_attributes(uint8_t *input_row) {
  row_ptr = this->row + 4 + 4 + 4;
  uint32_t ret = Record::consume_all_encrypted_attributes(input_row);
  row_ptr = this->row + 4 + 4;
  *( (uint32_t *) row_ptr) = this->num_cols;
  return ret;
}

// only the attributes!
uint32_t AggRecord::flush_encrypt_all_attributes(uint8_t *output) {
  uint8_t *input_ptr = this->row + 4 + 4 + 4;
  uint8_t *output_ptr = output;
  uint32_t value_len = 0;
  
  *( (uint32_t *) (output_ptr)) = this->num_cols;
  output_ptr += 4;

  //printf("flush_encrypt_all_attributes\n");
  for (uint32_t i = 0; i < this->num_cols; i++) {
    encrypt_attribute(&input_ptr, &output_ptr);
	
    // value_len = *( (uint32_t *) (input_ptr + TYPE_SIZE));
    // *( (uint32_t *) output_ptr ) = enc_size(value_len + HEADER_SIZE);
	
    // encrypt(input_ptr, value_len + HEADER_SIZE, output_ptr + 4);
	
    // input_ptr += value_len + HEADER_SIZE;
    // output_ptr += enc_size(value_len + HEADER_SIZE) + 4;
  }

  return (output_ptr - output);
}

void AggRecord::flush() {
  *( (uint32_t *) (row + 4 + 4)) = this->num_cols;
}
 
/*** AGG SORT RECORD  ***/
