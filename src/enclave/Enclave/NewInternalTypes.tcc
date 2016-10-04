// -*- mode: C++ -*-

template<typename Type>
void NewRecord::add_attr_val(Type value, bool dummy) {
  row_length += write_attr<Type>(row + row_length, value, dummy);
  set_num_cols(num_cols() + 1);
}
