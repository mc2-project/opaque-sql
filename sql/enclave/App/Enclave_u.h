#ifndef ENCLAVE_U_H__
#define ENCLAVE_U_H__

#include <stdint.h>
#include <wchar.h>
#include <stddef.h>
#include <string.h>
#include "sgx_edger8r.h" /* for sgx_satus_t etc. */

#include "user_types.h"

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

void SGX_UBRIDGE(SGX_NOCONVENTION, ocall_print_string, (const char* str));
void SGX_UBRIDGE(SGX_NOCONVENTION, ocall_pointer_user_check, (int* val));
void SGX_UBRIDGE(SGX_NOCONVENTION, ocall_pointer_in, (int* val));
void SGX_UBRIDGE(SGX_NOCONVENTION, ocall_pointer_out, (int* val));
void SGX_UBRIDGE(SGX_NOCONVENTION, ocall_pointer_in_out, (int* val));
SGX_DLLIMPORT void* SGX_UBRIDGE(SGX_CDECL, memccpy, (void* dest, const void* src, int val, size_t len));
void SGX_UBRIDGE(SGX_NOCONVENTION, ocall_function_allow, ());
void SGX_UBRIDGE(SGX_CDECL, sgx_oc_cpuidex, (int cpuinfo[4], int leaf, int subleaf));
int SGX_UBRIDGE(SGX_CDECL, sgx_thread_wait_untrusted_event_ocall, (const void* self));
int SGX_UBRIDGE(SGX_CDECL, sgx_thread_set_untrusted_event_ocall, (const void* waiter));
int SGX_UBRIDGE(SGX_CDECL, sgx_thread_setwait_untrusted_events_ocall, (const void* waiter, const void* self));
int SGX_UBRIDGE(SGX_CDECL, sgx_thread_set_multiple_untrusted_events_ocall, (const void** waiters, size_t total));

sgx_status_t ecall_filter_single_row(sgx_enclave_id_t eid, int* retval, int op_code, uint8_t* row, uint32_t length);
sgx_status_t ecall_encrypt(sgx_enclave_id_t eid, uint8_t* plaintext, uint32_t length, uint8_t* ciphertext, uint32_t cipher_length);
sgx_status_t ecall_decrypt(sgx_enclave_id_t eid, uint8_t* ciphertext, uint32_t ciphertext_length, uint8_t* plaintext, uint32_t plaintext_length);
sgx_status_t ecall_test_int(sgx_enclave_id_t eid, int* ptr);
sgx_status_t ecall_oblivious_sort_int(sgx_enclave_id_t eid, int* input, uint32_t input_len);
sgx_status_t ecall_oblivious_sort(sgx_enclave_id_t eid, int op_code, uint8_t* input, uint32_t buffer_length, int low_idx, uint32_t list_length);
sgx_status_t ecall_random_id(sgx_enclave_id_t eid, uint8_t* ptr, uint32_t length);
sgx_status_t ecall_scan_aggregation_count_distinct(sgx_enclave_id_t eid, int op_code, uint8_t* input_rows, uint32_t input_rows_length, uint32_t num_rows, uint8_t* agg_row, uint32_t agg_row_buffer_length, uint8_t* output_rows, uint32_t output_rows_length, uint32_t* actual_size, int flag, uint32_t* cardinality);
sgx_status_t ecall_test(sgx_enclave_id_t eid);
sgx_status_t ecall_process_boundary_records(sgx_enclave_id_t eid, int op_code, uint8_t* rows, uint32_t rows_size, uint32_t num_rows, uint8_t* out_agg_rows, uint32_t out_agg_row_size, uint32_t* actual_out_agg_row_size);
sgx_status_t ecall_final_aggregation(sgx_enclave_id_t eid, int op_code, uint8_t* agg_rows, uint32_t agg_rows_length, uint32_t num_rows, uint8_t* ret, uint32_t ret_length);
sgx_status_t ecall_scan_collect_last_primary(sgx_enclave_id_t eid, int op_code, uint8_t* input_rows, uint32_t input_rows_length, uint32_t num_rows, uint8_t* output, uint32_t output_length);
sgx_status_t ecall_process_join_boundary(sgx_enclave_id_t eid, uint8_t* input_rows, uint32_t input_rows_length, uint32_t num_rows, uint8_t* output_rows, uint32_t output_rows_size, uint8_t* enc_table_p, uint8_t* enc_table_f);
sgx_status_t ecall_sort_merge_join(sgx_enclave_id_t eid, int op_code, uint8_t* input_rows, uint32_t input_rows_length, uint32_t num_rows, uint8_t* join_row, uint32_t join_row_length, uint8_t* output_rows, uint32_t output_rows_length);
sgx_status_t ecall_join_sort_preprocess(sgx_enclave_id_t eid, int op_code, uint8_t* table_id, uint8_t* input_row, uint32_t input_row_len, uint32_t num_rows, uint8_t* output_row, uint32_t output_row_len);
sgx_status_t ecall_type_char(sgx_enclave_id_t eid, char val);
sgx_status_t ecall_type_int(sgx_enclave_id_t eid, int val);
sgx_status_t ecall_type_float(sgx_enclave_id_t eid, float val);
sgx_status_t ecall_type_double(sgx_enclave_id_t eid, double val);
sgx_status_t ecall_type_size_t(sgx_enclave_id_t eid, size_t val);
sgx_status_t ecall_type_wchar_t(sgx_enclave_id_t eid, wchar_t val);
sgx_status_t ecall_type_struct(sgx_enclave_id_t eid, struct struct_foo_t val);
sgx_status_t ecall_type_enum_union(sgx_enclave_id_t eid, enum enum_foo_t val1, union union_foo_t* val2);
sgx_status_t ecall_pointer_user_check(sgx_enclave_id_t eid, size_t* retval, void* val, size_t sz);
sgx_status_t ecall_pointer_in(sgx_enclave_id_t eid, int* val);
sgx_status_t ecall_pointer_out(sgx_enclave_id_t eid, int* val);
sgx_status_t ecall_pointer_in_out(sgx_enclave_id_t eid, int* val);
sgx_status_t ecall_pointer_string(sgx_enclave_id_t eid, char* str);
sgx_status_t ecall_pointer_string_const(sgx_enclave_id_t eid, const char* str);
sgx_status_t ecall_pointer_size(sgx_enclave_id_t eid, void* ptr, size_t len);
sgx_status_t ecall_pointer_count(sgx_enclave_id_t eid, int* arr, int cnt);
sgx_status_t ecall_pointer_isptr_readonly(sgx_enclave_id_t eid, buffer_t buf, size_t len);
sgx_status_t ecall_pointer_sizefunc(sgx_enclave_id_t eid, char* buf);
sgx_status_t ocall_pointer_attr(sgx_enclave_id_t eid);
sgx_status_t ecall_array_user_check(sgx_enclave_id_t eid, int arr[4]);
sgx_status_t ecall_array_in(sgx_enclave_id_t eid, int arr[4]);
sgx_status_t ecall_array_out(sgx_enclave_id_t eid, int arr[4]);
sgx_status_t ecall_array_in_out(sgx_enclave_id_t eid, int arr[4]);
sgx_status_t ecall_array_isary(sgx_enclave_id_t eid, array_t arr);
sgx_status_t ecall_function_calling_convs(sgx_enclave_id_t eid);
sgx_status_t ecall_function_public(sgx_enclave_id_t eid);
sgx_status_t ecall_function_private(sgx_enclave_id_t eid, int* retval);
sgx_status_t ecall_malloc_free(sgx_enclave_id_t eid);
sgx_status_t ecall_sgx_cpuid(sgx_enclave_id_t eid, int cpuinfo[4], int leaf);
sgx_status_t ecall_exception(sgx_enclave_id_t eid);
sgx_status_t ecall_map(sgx_enclave_id_t eid);
sgx_status_t ecall_increase_counter(sgx_enclave_id_t eid, size_t* retval);
sgx_status_t ecall_producer(sgx_enclave_id_t eid);
sgx_status_t ecall_consumer(sgx_enclave_id_t eid);

#ifdef __cplusplus
}
#endif /* __cplusplus */

#endif
