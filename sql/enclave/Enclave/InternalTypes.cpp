#include "InternalTypes.h"

template<typename T>
int compare(T *value1, T *value2) {
  printf("compare(): Type not supported\n");
  assert(false);
}

template<typename T>
void compare_and_swap(T *value1, T* value2) {
  printf("compare_and_swap(): Type not supported\n");
  assert(false);
}


template<typename T>
void swap(T *value1, T* value2) {
  printf("swap(): Type not supported\n");
  assert(false);
}

// -1 means value1 < value2
// 0 means value1 == value2
// 1 means value1 > value2

template<>
int compare<Integer>(Integer *value1, Integer *value2) {
  if (value1->value < value2->value) {
	return -1;
  } else if (value1->value > value2->value) {
	return 1;
  } else {
	return 0;
  }
}

// TODO: to make this completely oblivious, we would need to swap even if
// you don't need to! How to simulate this?
template<>
void compare_and_swap<Integer>(Integer *value1, Integer *value2) {
  int comp = compare<Integer>(value1, value2);

  if (comp == 1) {
	int temp = value1->value;
	value1->value = value2->value;
	value2->value = temp;
  }
}

template<>
int compare<String>(String *value1, String *value2) {
  // String comparison is lexicographic order
  uint32_t min_size = value1->length < value2->length ? value1->length : value2->length;
  // printf("value1's length is %u\n", value1->length);
  // printf("value2's length is %u\n", value2->length);
  // print_bytes(value1->buffer, value1->length);
  // print_bytes(value2->buffer, value2->length);
  int ret = 0;
  for (uint32_t i = 0; i < min_size; i++) {
	if (*(value1->buffer+i) < *(value2->buffer+i)) {
	  return -1;
	} else if (*(value1->buffer+i) > *(value2->buffer+i)) {
	  return 1;
	}
  }

  if (value1->length < value2->length) {
	return -1;
  } else if (value1->length > value2->length) {
	return 1;
  }

  // these two strings are exactly equal!
  return ret;
}

template<>
void compare_and_swap<String>(String *value1, String *value2) {
  int comp = compare<String>(value1, value2);
  
  if (comp == 1) {
	uint32_t temp_len = value1->length;
	uint8_t *temp_buf = value1->buffer;
	value1->length = value2->length;
	value1->buffer = value2->buffer;
	value2->length = temp_len;
	value2->buffer = temp_buf;
  }
}

/** start Record functions **/
template<>
void swap<Record>(Record *value1, Record* value2) {
  uint32_t num_cols = value1->num_cols;
  uint8_t *data = value1->data;
  uint8_t *sort_attribute = value1->sort_attribute;
  // swap
  value1->num_cols = value2->num_cols;
  value1->data = value2->data;
  value1->sort_attribute = value2->sort_attribute;
  
  value2->num_cols = num_cols;
  value2->data = data;
  value2->sort_attribute = sort_attribute;
}

template<>
int compare<Record>(Record *value1, Record *value2) {
  uint8_t type1 = *(value1->sort_attribute);
  uint8_t type2 = *(value2->sort_attribute);

  //printf("Comparing %p and %p, type 1 is %u, type 2 is %u\n", value1->sort_attribute, value2->sort_attribute, type1, type2);

  assert(type1 == type2);

  switch(type1) {
  case 1:
	{
	  // Integer
	  Integer *int1 = (Integer *) (value1->sort_attribute + HEADER_SIZE);
	  Integer *int2 = (Integer *) (value2->sort_attribute + HEADER_SIZE);
	  return compare<Integer>(int1, int2);
	}
	break;
  case 2:
	{
	  // String
	  String str1;
	  str1.length = *( (uint32_t *) (value1->sort_attribute + TYPE_SIZE));
	  str1.buffer = (value1->sort_attribute + HEADER_SIZE);
	  
	  String str2;
	  str2.length = *( (uint32_t*) (value2->sort_attribute + TYPE_SIZE));
	  str2.buffer = (value2->sort_attribute + HEADER_SIZE);
	  return compare<String>(&str1, &str2);
	}
	break;
  }
}


template<>
void compare_and_swap<Record>(Record *value1, Record *value2) {
  int comp = compare<Record>(value1, value2);
  
  if (comp == 1) {
	swap<Record>(value1, value2);
  }
}

/** end Record functions **/

/** Begin JoinRecord **/

template<>
int compare<JoinRecord>(JoinRecord *value1, JoinRecord *value2) {

  // printf("Comparing two records %u and %d\n", value1->if_primary, value2->if_primary);
  // print_attribute("1", (value1->record).sort_attribute);
  // print_attribute("2", (value2->record).sort_attribute);
  //print_join_row("1", (value1->record).data);
  //print_join_row("2", (value2->record).data);
  int ret = compare<Record>(&(value1->record), &(value2->record));
  //printf("Result is %u\n", ret);
  
  if (ret == 0) {
	// compare the table type
	// only one of them should be primary
	if (value1->if_primary == 0) {
	  return -1;
	} else if (value2->if_primary == 0) {
	  return 1;
	} else {
	  // this means both are foreign tables
	  return 0;
	}
  }
  return ret;
}

template<>
void compare_and_swap<JoinRecord>(JoinRecord *value1, JoinRecord *value2) {
  int comp = compare<JoinRecord>(value1, value2);
  
  if (comp == 1) {
	int if_primary = value1->if_primary;
	value1->if_primary = value2->if_primary;
	value2->if_primary = if_primary;
	swap<Record>(&(value1->record), &(value2->record));
  }
}

/** End JoinRecord **/
