// -*- mode: C++ -*-

template <typename AttrGeneratorType>
void NewRecord::add_attr(const AttrGeneratorType *attr) {
  this->row_length += attr->write_result(this->row + this->row_length);
  (*reinterpret_cast<uint32_t *>(this->row))++;
}

template <typename AttrGeneratorType>
void NewRecord::add_attr(const AttrGeneratorType *attr, bool dummy) {
  this->row_length += attr->write_result(this->row + this->row_length, dummy);
  (*reinterpret_cast<uint32_t *>(this->row))++;
}
