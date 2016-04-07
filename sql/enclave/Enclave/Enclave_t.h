#ifndef ENCLAVE_T_H__
#define ENCLAVE_T_H__

#include <stdint.h>
#include <wchar.h>
#include <stddef.h>
#include "sgx_edger8r.h" /* for sgx_ocall etc. */

#include "user_types.h"
#include "jni.h"
#include "jni_md.h"

#include <stdlib.h> /* for size_t */

#define SGX_CAST(type, item) ((type)(item))

#ifdef __cplusplus
extern "C" {
#endif

typedef struct struct_foo_t {
	uint32_t struct_foo_0;
	uint64_t struct_foo_1;
} struct_foo_t;

typedef enum enum_foo_t {
	ENUM_FOO_0 = 0,
	ENUM_FOO_1 = 1,
} enum_foo_t;

typedef union union_foo_t {
	uint32_t union_foo_0;
	uint32_t union_foo_1;
	uint64_t union_foo_3;
} union_foo_t;

size_t get_buffer_len(const char* val);

int ecall_filter_single_row(int op_code, uint8_t* row, uint32_t length);
void ecall_encrypt(uint8_t* plaintext, uint32_t length, uint8_t* ciphertext, uint32_t cipher_length);
void ecall_decrypt(uint8_t* ciphertext, uint32_t ciphertext_length, uint8_t* plaintext, uint32_t plaintext_length);
void ecall_test_int(int* ptr);
void ecall_oblivious_sort_int(int* input, uint32_t input_len);
void ecall_oblivious_sort(int op_code, uint8_t* input, uint32_t buffer_length, int low_idx, uint32_t list_length);
void ecall_random_id(uint8_t* ptr, uint32_t length);
void ecall_scan_aggregation_count_distinct(int op_code, uint8_t* input_rows, uint32_t input_rows_length, uint32_t num_rows, uint8_t* agg_row, uint32_t agg_row_buffer_length, uint8_t* output_rows, uint32_t output_rows_length);
void ecall_test();
void ecall_type_char(char val);
void ecall_type_int(int val);
void ecall_type_float(float val);
void ecall_type_double(double val);
void ecall_type_size_t(size_t val);
void ecall_type_wchar_t(wchar_t val);
void ecall_type_struct(struct struct_foo_t val);
void ecall_type_enum_union(enum enum_foo_t val1, union union_foo_t* val2);
size_t ecall_pointer_user_check(void* val, size_t sz);
void ecall_pointer_in(int* val);
void ecall_pointer_out(int* val);
void ecall_pointer_in_out(int* val);
void ecall_pointer_string(char* str);
void ecall_pointer_string_const(const char* str);
void ecall_pointer_size(void* ptr, size_t len);
void ecall_pointer_count(int* arr, int cnt);
void ecall_pointer_isptr_readonly(buffer_t buf, size_t len);
void ecall_pointer_sizefunc(char* buf);
void ocall_pointer_attr();
void ecall_array_user_check(int arr[4]);
void ecall_array_in(int arr[4]);
void ecall_array_out(int arr[4]);
void ecall_array_in_out(int arr[4]);
void ecall_array_isary(array_t arr);
void ecall_function_calling_convs();
void ecall_function_public();
int ecall_function_private();
void ecall_malloc_free();
void ecall_sgx_cpuid(int cpuinfo[4], int leaf);
void ecall_exception();
void ecall_map();
size_t ecall_increase_counter();
void ecall_producer();
void ecall_consumer();

sgx_status_t SGX_CDECL ocall_print_string(const char* str);
sgx_status_t SGX_CDECL ocall_pointer_user_check(int* val);
sgx_status_t SGX_CDECL ocall_pointer_in(int* val);
sgx_status_t SGX_CDECL ocall_pointer_out(int* val);
sgx_status_t SGX_CDECL ocall_pointer_in_out(int* val);
sgx_status_t SGX_CDECL memccpy(void** retval, void* dest, const void* src, int val, size_t len);
sgx_status_t SGX_CDECL ocall_function_allow();
sgx_status_t SGX_CDECL sgx_oc_cpuidex(int cpuinfo[4], int leaf, int subleaf);
sgx_status_t SGX_CDECL sgx_thread_wait_untrusted_event_ocall(int* retval, const void* self);
sgx_status_t SGX_CDECL sgx_thread_set_untrusted_event_ocall(int* retval, const void* waiter);
sgx_status_t SGX_CDECL sgx_thread_setwait_untrusted_events_ocall(int* retval, const void* waiter, const void* self);
sgx_status_t SGX_CDECL sgx_thread_set_multiple_untrusted_events_ocall(int* retval, const void** waiters, size_t total);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif
