#include "Enclave_t.h"

#include "sgx_trts.h" /* for sgx_ocalloc, sgx_is_outside_enclave */

#include <errno.h>
#include <string.h> /* for memcpy etc */
#include <stdlib.h> /* for malloc/free etc */

#define CHECK_REF_POINTER(ptr, siz) do {	\
	if (!(ptr) || ! sgx_is_outside_enclave((ptr), (siz)))	\
		return SGX_ERROR_INVALID_PARAMETER;\
} while (0)

#define CHECK_UNIQUE_POINTER(ptr, siz) do {	\
	if ((ptr) && ! sgx_is_outside_enclave((ptr), (siz)))	\
		return SGX_ERROR_INVALID_PARAMETER;\
} while (0)


typedef struct ms_ecall_filter_single_row_t {
	int ms_retval;
	int ms_op_code;
	uint8_t* ms_row;
	uint32_t ms_length;
} ms_ecall_filter_single_row_t;

typedef struct ms_ecall_encrypt_t {
	uint8_t* ms_plaintext;
	uint32_t ms_length;
	uint8_t* ms_ciphertext;
	uint32_t ms_cipher_length;
} ms_ecall_encrypt_t;

typedef struct ms_ecall_decrypt_t {
	uint8_t* ms_ciphertext;
	uint32_t ms_ciphertext_length;
	uint8_t* ms_plaintext;
	uint32_t ms_plaintext_length;
} ms_ecall_decrypt_t;

typedef struct ms_ecall_test_int_t {
	int* ms_ptr;
} ms_ecall_test_int_t;

typedef struct ms_ecall_oblivious_sort_int_t {
	int* ms_input;
	uint32_t ms_input_len;
} ms_ecall_oblivious_sort_int_t;

typedef struct ms_ecall_oblivious_sort_t {
	int ms_op_code;
	uint8_t* ms_input;
	uint32_t ms_buffer_length;
	int ms_low_idx;
	uint32_t ms_list_length;
} ms_ecall_oblivious_sort_t;

typedef struct ms_ecall_random_id_t {
	uint8_t* ms_ptr;
	uint32_t ms_length;
} ms_ecall_random_id_t;

typedef struct ms_ecall_scan_aggregation_count_distinct_t {
	int ms_op_code;
	uint8_t* ms_input_rows;
	uint32_t ms_input_rows_length;
	uint32_t ms_num_rows;
	uint8_t* ms_agg_row;
	uint32_t ms_agg_row_buffer_length;
	uint8_t* ms_output_rows;
	uint32_t ms_output_rows_length;
	uint32_t* ms_actual_size;
	int ms_flag;
	uint32_t* ms_cardinality;
} ms_ecall_scan_aggregation_count_distinct_t;


typedef struct ms_ecall_process_boundary_records_t {
	int ms_op_code;
	uint8_t* ms_rows;
	uint32_t ms_rows_size;
	uint32_t ms_num_rows;
	uint8_t* ms_out_agg_rows;
	uint32_t ms_out_agg_row_size;
	uint32_t* ms_actual_out_agg_row_size;
} ms_ecall_process_boundary_records_t;

typedef struct ms_ecall_final_aggregation_t {
	int ms_op_code;
	uint8_t* ms_agg_rows;
	uint32_t ms_agg_rows_length;
	uint32_t ms_num_rows;
	uint8_t* ms_ret;
	uint32_t ms_ret_length;
} ms_ecall_final_aggregation_t;

typedef struct ms_ecall_scan_collect_last_primary_t {
	int ms_op_code;
	uint8_t* ms_input_rows;
	uint32_t ms_input_rows_length;
	uint32_t ms_num_rows;
	uint8_t* ms_output;
	uint32_t ms_output_length;
} ms_ecall_scan_collect_last_primary_t;

typedef struct ms_ecall_process_join_boundary_t {
	uint8_t* ms_input_rows;
	uint32_t ms_input_rows_length;
	uint32_t ms_num_rows;
	uint8_t* ms_output_rows;
	uint32_t ms_output_rows_size;
	uint8_t* ms_enc_table_p;
	uint8_t* ms_enc_table_f;
} ms_ecall_process_join_boundary_t;

typedef struct ms_ecall_sort_merge_join_t {
	int ms_op_code;
	uint8_t* ms_input_rows;
	uint32_t ms_input_rows_length;
	uint32_t ms_num_rows;
	uint8_t* ms_join_row;
	uint32_t ms_join_row_length;
	uint8_t* ms_output_rows;
	uint32_t ms_output_rows_length;
	uint32_t* ms_actual_output_length;
} ms_ecall_sort_merge_join_t;

typedef struct ms_ecall_join_sort_preprocess_t {
	int ms_op_code;
	uint8_t* ms_table_id;
	uint8_t* ms_input_row;
	uint32_t ms_input_row_len;
	uint32_t ms_num_rows;
	uint8_t* ms_output_row;
	uint32_t ms_output_row_len;
} ms_ecall_join_sort_preprocess_t;

typedef struct ms_ecall_type_char_t {
	char ms_val;
} ms_ecall_type_char_t;

typedef struct ms_ecall_type_int_t {
	int ms_val;
} ms_ecall_type_int_t;

typedef struct ms_ecall_type_float_t {
	float ms_val;
} ms_ecall_type_float_t;

typedef struct ms_ecall_type_double_t {
	double ms_val;
} ms_ecall_type_double_t;

typedef struct ms_ecall_type_size_t_t {
	size_t ms_val;
} ms_ecall_type_size_t_t;

typedef struct ms_ecall_type_wchar_t_t {
	wchar_t ms_val;
} ms_ecall_type_wchar_t_t;

typedef struct ms_ecall_type_struct_t {
	struct struct_foo_t ms_val;
} ms_ecall_type_struct_t;

typedef struct ms_ecall_type_enum_union_t {
	enum enum_foo_t ms_val1;
	union union_foo_t* ms_val2;
} ms_ecall_type_enum_union_t;

typedef struct ms_ecall_pointer_user_check_t {
	size_t ms_retval;
	void* ms_val;
	size_t ms_sz;
} ms_ecall_pointer_user_check_t;

typedef struct ms_ecall_pointer_in_t {
	int* ms_val;
} ms_ecall_pointer_in_t;

typedef struct ms_ecall_pointer_out_t {
	int* ms_val;
} ms_ecall_pointer_out_t;

typedef struct ms_ecall_pointer_in_out_t {
	int* ms_val;
} ms_ecall_pointer_in_out_t;

typedef struct ms_ecall_pointer_string_t {
	char* ms_str;
} ms_ecall_pointer_string_t;

typedef struct ms_ecall_pointer_string_const_t {
	char* ms_str;
} ms_ecall_pointer_string_const_t;

typedef struct ms_ecall_pointer_size_t {
	void* ms_ptr;
	size_t ms_len;
} ms_ecall_pointer_size_t;

typedef struct ms_ecall_pointer_count_t {
	int* ms_arr;
	int ms_cnt;
} ms_ecall_pointer_count_t;

typedef struct ms_ecall_pointer_isptr_readonly_t {
	buffer_t ms_buf;
	size_t ms_len;
} ms_ecall_pointer_isptr_readonly_t;

typedef struct ms_ecall_pointer_sizefunc_t {
	char* ms_buf;
} ms_ecall_pointer_sizefunc_t;


typedef struct ms_ecall_array_user_check_t {
	int* ms_arr;
} ms_ecall_array_user_check_t;

typedef struct ms_ecall_array_in_t {
	int* ms_arr;
} ms_ecall_array_in_t;

typedef struct ms_ecall_array_out_t {
	int* ms_arr;
} ms_ecall_array_out_t;

typedef struct ms_ecall_array_in_out_t {
	int* ms_arr;
} ms_ecall_array_in_out_t;

typedef struct ms_ecall_array_isary_t {
	array_t*  ms_arr;
} ms_ecall_array_isary_t;



typedef struct ms_ecall_function_private_t {
	int ms_retval;
} ms_ecall_function_private_t;


typedef struct ms_ecall_sgx_cpuid_t {
	int* ms_cpuinfo;
	int ms_leaf;
} ms_ecall_sgx_cpuid_t;



typedef struct ms_ecall_increase_counter_t {
	size_t ms_retval;
} ms_ecall_increase_counter_t;



typedef struct ms_ocall_print_string_t {
	char* ms_str;
} ms_ocall_print_string_t;

typedef struct ms_ocall_pointer_user_check_t {
	int* ms_val;
} ms_ocall_pointer_user_check_t;

typedef struct ms_ocall_pointer_in_t {
	int* ms_val;
} ms_ocall_pointer_in_t;

typedef struct ms_ocall_pointer_out_t {
	int* ms_val;
} ms_ocall_pointer_out_t;

typedef struct ms_ocall_pointer_in_out_t {
	int* ms_val;
} ms_ocall_pointer_in_out_t;

typedef struct ms_memccpy_t {
	void* ms_retval;
	void* ms_dest;
	void* ms_src;
	int ms_val;
	size_t ms_len;
} ms_memccpy_t;


typedef struct ms_sgx_oc_cpuidex_t {
	int* ms_cpuinfo;
	int ms_leaf;
	int ms_subleaf;
} ms_sgx_oc_cpuidex_t;

typedef struct ms_sgx_thread_wait_untrusted_event_ocall_t {
	int ms_retval;
	void* ms_self;
} ms_sgx_thread_wait_untrusted_event_ocall_t;

typedef struct ms_sgx_thread_set_untrusted_event_ocall_t {
	int ms_retval;
	void* ms_waiter;
} ms_sgx_thread_set_untrusted_event_ocall_t;

typedef struct ms_sgx_thread_setwait_untrusted_events_ocall_t {
	int ms_retval;
	void* ms_waiter;
	void* ms_self;
} ms_sgx_thread_setwait_untrusted_events_ocall_t;

typedef struct ms_sgx_thread_set_multiple_untrusted_events_ocall_t {
	int ms_retval;
	void** ms_waiters;
	size_t ms_total;
} ms_sgx_thread_set_multiple_untrusted_events_ocall_t;

static sgx_status_t SGX_CDECL sgx_ecall_filter_single_row(void* pms)
{
	ms_ecall_filter_single_row_t* ms = SGX_CAST(ms_ecall_filter_single_row_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	uint8_t* _tmp_row = ms->ms_row;
	uint32_t _tmp_length = ms->ms_length;
	size_t _len_row = _tmp_length;
	uint8_t* _in_row = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_filter_single_row_t));
	CHECK_UNIQUE_POINTER(_tmp_row, _len_row);

	if (_tmp_row != NULL) {
		_in_row = (uint8_t*)malloc(_len_row);
		if (_in_row == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_row, _tmp_row, _len_row);
	}
	ms->ms_retval = ecall_filter_single_row(ms->ms_op_code, _in_row, _tmp_length);
err:
	if (_in_row) free(_in_row);

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_encrypt(void* pms)
{
	ms_ecall_encrypt_t* ms = SGX_CAST(ms_ecall_encrypt_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	uint8_t* _tmp_plaintext = ms->ms_plaintext;
	uint32_t _tmp_length = ms->ms_length;
	size_t _len_plaintext = _tmp_length;
	uint8_t* _in_plaintext = NULL;
	uint8_t* _tmp_ciphertext = ms->ms_ciphertext;
	uint32_t _tmp_cipher_length = ms->ms_cipher_length;
	size_t _len_ciphertext = _tmp_cipher_length;
	uint8_t* _in_ciphertext = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_encrypt_t));
	CHECK_UNIQUE_POINTER(_tmp_plaintext, _len_plaintext);
	CHECK_UNIQUE_POINTER(_tmp_ciphertext, _len_ciphertext);

	if (_tmp_plaintext != NULL) {
		_in_plaintext = (uint8_t*)malloc(_len_plaintext);
		if (_in_plaintext == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_plaintext, _tmp_plaintext, _len_plaintext);
	}
	if (_tmp_ciphertext != NULL) {
		if ((_in_ciphertext = (uint8_t*)malloc(_len_ciphertext)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_ciphertext, 0, _len_ciphertext);
	}
	ecall_encrypt(_in_plaintext, _tmp_length, _in_ciphertext, _tmp_cipher_length);
err:
	if (_in_plaintext) free(_in_plaintext);
	if (_in_ciphertext) {
		memcpy(_tmp_ciphertext, _in_ciphertext, _len_ciphertext);
		free(_in_ciphertext);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_decrypt(void* pms)
{
	ms_ecall_decrypt_t* ms = SGX_CAST(ms_ecall_decrypt_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	uint8_t* _tmp_ciphertext = ms->ms_ciphertext;
	uint32_t _tmp_ciphertext_length = ms->ms_ciphertext_length;
	size_t _len_ciphertext = _tmp_ciphertext_length;
	uint8_t* _in_ciphertext = NULL;
	uint8_t* _tmp_plaintext = ms->ms_plaintext;
	uint32_t _tmp_plaintext_length = ms->ms_plaintext_length;
	size_t _len_plaintext = _tmp_plaintext_length;
	uint8_t* _in_plaintext = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_decrypt_t));
	CHECK_UNIQUE_POINTER(_tmp_ciphertext, _len_ciphertext);
	CHECK_UNIQUE_POINTER(_tmp_plaintext, _len_plaintext);

	if (_tmp_ciphertext != NULL) {
		_in_ciphertext = (uint8_t*)malloc(_len_ciphertext);
		if (_in_ciphertext == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_ciphertext, _tmp_ciphertext, _len_ciphertext);
	}
	if (_tmp_plaintext != NULL) {
		if ((_in_plaintext = (uint8_t*)malloc(_len_plaintext)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_plaintext, 0, _len_plaintext);
	}
	ecall_decrypt(_in_ciphertext, _tmp_ciphertext_length, _in_plaintext, _tmp_plaintext_length);
err:
	if (_in_ciphertext) free(_in_ciphertext);
	if (_in_plaintext) {
		memcpy(_tmp_plaintext, _in_plaintext, _len_plaintext);
		free(_in_plaintext);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_test_int(void* pms)
{
	ms_ecall_test_int_t* ms = SGX_CAST(ms_ecall_test_int_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	int* _tmp_ptr = ms->ms_ptr;
	size_t _len_ptr = 1;
	int* _in_ptr = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_test_int_t));
	CHECK_UNIQUE_POINTER(_tmp_ptr, _len_ptr);

	if (_tmp_ptr != NULL) {
		_in_ptr = (int*)malloc(_len_ptr);
		if (_in_ptr == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_ptr, _tmp_ptr, _len_ptr);
	}
	ecall_test_int(_in_ptr);
err:
	if (_in_ptr) free(_in_ptr);

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_oblivious_sort_int(void* pms)
{
	ms_ecall_oblivious_sort_int_t* ms = SGX_CAST(ms_ecall_oblivious_sort_int_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	int* _tmp_input = ms->ms_input;
	uint32_t _tmp_input_len = ms->ms_input_len;
	size_t _len_input = _tmp_input_len * sizeof(*_tmp_input);
	int* _in_input = NULL;

	if ((size_t)_tmp_input_len > (SIZE_MAX / sizeof(*_tmp_input))) {
		status = SGX_ERROR_INVALID_PARAMETER;
		goto err;
	}

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_oblivious_sort_int_t));
	CHECK_UNIQUE_POINTER(_tmp_input, _len_input);

	if (_tmp_input != NULL) {
		_in_input = (int*)malloc(_len_input);
		if (_in_input == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_input, _tmp_input, _len_input);
	}
	ecall_oblivious_sort_int(_in_input, _tmp_input_len);
err:
	if (_in_input) {
		memcpy(_tmp_input, _in_input, _len_input);
		free(_in_input);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_oblivious_sort(void* pms)
{
	ms_ecall_oblivious_sort_t* ms = SGX_CAST(ms_ecall_oblivious_sort_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	uint8_t* _tmp_input = ms->ms_input;
	uint32_t _tmp_buffer_length = ms->ms_buffer_length;
	size_t _len_input = _tmp_buffer_length;
	uint8_t* _in_input = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_oblivious_sort_t));
	CHECK_UNIQUE_POINTER(_tmp_input, _len_input);

	if (_tmp_input != NULL) {
		_in_input = (uint8_t*)malloc(_len_input);
		if (_in_input == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_input, _tmp_input, _len_input);
	}
	ecall_oblivious_sort(ms->ms_op_code, _in_input, _tmp_buffer_length, ms->ms_low_idx, ms->ms_list_length);
err:
	if (_in_input) {
		memcpy(_tmp_input, _in_input, _len_input);
		free(_in_input);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_random_id(void* pms)
{
	ms_ecall_random_id_t* ms = SGX_CAST(ms_ecall_random_id_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	uint8_t* _tmp_ptr = ms->ms_ptr;
	uint32_t _tmp_length = ms->ms_length;
	size_t _len_ptr = _tmp_length;
	uint8_t* _in_ptr = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_random_id_t));
	CHECK_UNIQUE_POINTER(_tmp_ptr, _len_ptr);

	if (_tmp_ptr != NULL) {
		_in_ptr = (uint8_t*)malloc(_len_ptr);
		if (_in_ptr == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_ptr, _tmp_ptr, _len_ptr);
	}
	ecall_random_id(_in_ptr, _tmp_length);
err:
	if (_in_ptr) {
		memcpy(_tmp_ptr, _in_ptr, _len_ptr);
		free(_in_ptr);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_scan_aggregation_count_distinct(void* pms)
{
	ms_ecall_scan_aggregation_count_distinct_t* ms = SGX_CAST(ms_ecall_scan_aggregation_count_distinct_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	uint8_t* _tmp_input_rows = ms->ms_input_rows;
	uint32_t _tmp_input_rows_length = ms->ms_input_rows_length;
	size_t _len_input_rows = _tmp_input_rows_length;
	uint8_t* _in_input_rows = NULL;
	uint8_t* _tmp_agg_row = ms->ms_agg_row;
	uint32_t _tmp_agg_row_buffer_length = ms->ms_agg_row_buffer_length;
	size_t _len_agg_row = _tmp_agg_row_buffer_length;
	uint8_t* _in_agg_row = NULL;
	uint8_t* _tmp_output_rows = ms->ms_output_rows;
	uint32_t _tmp_output_rows_length = ms->ms_output_rows_length;
	size_t _len_output_rows = _tmp_output_rows_length;
	uint8_t* _in_output_rows = NULL;
	uint32_t* _tmp_actual_size = ms->ms_actual_size;
	size_t _len_actual_size = sizeof(*_tmp_actual_size);
	uint32_t* _in_actual_size = NULL;
	uint32_t* _tmp_cardinality = ms->ms_cardinality;
	size_t _len_cardinality = sizeof(*_tmp_cardinality);
	uint32_t* _in_cardinality = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_scan_aggregation_count_distinct_t));
	CHECK_UNIQUE_POINTER(_tmp_input_rows, _len_input_rows);
	CHECK_UNIQUE_POINTER(_tmp_agg_row, _len_agg_row);
	CHECK_UNIQUE_POINTER(_tmp_output_rows, _len_output_rows);
	CHECK_UNIQUE_POINTER(_tmp_actual_size, _len_actual_size);
	CHECK_UNIQUE_POINTER(_tmp_cardinality, _len_cardinality);

	if (_tmp_input_rows != NULL) {
		_in_input_rows = (uint8_t*)malloc(_len_input_rows);
		if (_in_input_rows == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_input_rows, _tmp_input_rows, _len_input_rows);
	}
	if (_tmp_agg_row != NULL) {
		_in_agg_row = (uint8_t*)malloc(_len_agg_row);
		if (_in_agg_row == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_agg_row, _tmp_agg_row, _len_agg_row);
	}
	if (_tmp_output_rows != NULL) {
		if ((_in_output_rows = (uint8_t*)malloc(_len_output_rows)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_output_rows, 0, _len_output_rows);
	}
	if (_tmp_actual_size != NULL) {
		if ((_in_actual_size = (uint32_t*)malloc(_len_actual_size)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_actual_size, 0, _len_actual_size);
	}
	if (_tmp_cardinality != NULL) {
		if ((_in_cardinality = (uint32_t*)malloc(_len_cardinality)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_cardinality, 0, _len_cardinality);
	}
	ecall_scan_aggregation_count_distinct(ms->ms_op_code, _in_input_rows, _tmp_input_rows_length, ms->ms_num_rows, _in_agg_row, _tmp_agg_row_buffer_length, _in_output_rows, _tmp_output_rows_length, _in_actual_size, ms->ms_flag, _in_cardinality);
err:
	if (_in_input_rows) free(_in_input_rows);
	if (_in_agg_row) free(_in_agg_row);
	if (_in_output_rows) {
		memcpy(_tmp_output_rows, _in_output_rows, _len_output_rows);
		free(_in_output_rows);
	}
	if (_in_actual_size) {
		memcpy(_tmp_actual_size, _in_actual_size, _len_actual_size);
		free(_in_actual_size);
	}
	if (_in_cardinality) {
		memcpy(_tmp_cardinality, _in_cardinality, _len_cardinality);
		free(_in_cardinality);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_test(void* pms)
{
	sgx_status_t status = SGX_SUCCESS;
	if (pms != NULL) return SGX_ERROR_INVALID_PARAMETER;
	ecall_test();
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_process_boundary_records(void* pms)
{
	ms_ecall_process_boundary_records_t* ms = SGX_CAST(ms_ecall_process_boundary_records_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	uint8_t* _tmp_rows = ms->ms_rows;
	uint32_t _tmp_rows_size = ms->ms_rows_size;
	size_t _len_rows = _tmp_rows_size;
	uint8_t* _in_rows = NULL;
	uint8_t* _tmp_out_agg_rows = ms->ms_out_agg_rows;
	uint32_t _tmp_out_agg_row_size = ms->ms_out_agg_row_size;
	size_t _len_out_agg_rows = _tmp_out_agg_row_size;
	uint8_t* _in_out_agg_rows = NULL;
	uint32_t* _tmp_actual_out_agg_row_size = ms->ms_actual_out_agg_row_size;
	size_t _len_actual_out_agg_row_size = 4;
	uint32_t* _in_actual_out_agg_row_size = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_process_boundary_records_t));
	CHECK_UNIQUE_POINTER(_tmp_rows, _len_rows);
	CHECK_UNIQUE_POINTER(_tmp_out_agg_rows, _len_out_agg_rows);
	CHECK_UNIQUE_POINTER(_tmp_actual_out_agg_row_size, _len_actual_out_agg_row_size);

	if (_tmp_rows != NULL) {
		_in_rows = (uint8_t*)malloc(_len_rows);
		if (_in_rows == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_rows, _tmp_rows, _len_rows);
	}
	if (_tmp_out_agg_rows != NULL) {
		if ((_in_out_agg_rows = (uint8_t*)malloc(_len_out_agg_rows)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_out_agg_rows, 0, _len_out_agg_rows);
	}
	if (_tmp_actual_out_agg_row_size != NULL) {
		if ((_in_actual_out_agg_row_size = (uint32_t*)malloc(_len_actual_out_agg_row_size)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_actual_out_agg_row_size, 0, _len_actual_out_agg_row_size);
	}
	ecall_process_boundary_records(ms->ms_op_code, _in_rows, _tmp_rows_size, ms->ms_num_rows, _in_out_agg_rows, _tmp_out_agg_row_size, _in_actual_out_agg_row_size);
err:
	if (_in_rows) free(_in_rows);
	if (_in_out_agg_rows) {
		memcpy(_tmp_out_agg_rows, _in_out_agg_rows, _len_out_agg_rows);
		free(_in_out_agg_rows);
	}
	if (_in_actual_out_agg_row_size) {
		memcpy(_tmp_actual_out_agg_row_size, _in_actual_out_agg_row_size, _len_actual_out_agg_row_size);
		free(_in_actual_out_agg_row_size);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_final_aggregation(void* pms)
{
	ms_ecall_final_aggregation_t* ms = SGX_CAST(ms_ecall_final_aggregation_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	uint8_t* _tmp_agg_rows = ms->ms_agg_rows;
	uint32_t _tmp_agg_rows_length = ms->ms_agg_rows_length;
	size_t _len_agg_rows = _tmp_agg_rows_length;
	uint8_t* _in_agg_rows = NULL;
	uint8_t* _tmp_ret = ms->ms_ret;
	uint32_t _tmp_ret_length = ms->ms_ret_length;
	size_t _len_ret = _tmp_ret_length;
	uint8_t* _in_ret = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_final_aggregation_t));
	CHECK_UNIQUE_POINTER(_tmp_agg_rows, _len_agg_rows);
	CHECK_UNIQUE_POINTER(_tmp_ret, _len_ret);

	if (_tmp_agg_rows != NULL) {
		_in_agg_rows = (uint8_t*)malloc(_len_agg_rows);
		if (_in_agg_rows == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_agg_rows, _tmp_agg_rows, _len_agg_rows);
	}
	if (_tmp_ret != NULL) {
		if ((_in_ret = (uint8_t*)malloc(_len_ret)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_ret, 0, _len_ret);
	}
	ecall_final_aggregation(ms->ms_op_code, _in_agg_rows, _tmp_agg_rows_length, ms->ms_num_rows, _in_ret, _tmp_ret_length);
err:
	if (_in_agg_rows) free(_in_agg_rows);
	if (_in_ret) {
		memcpy(_tmp_ret, _in_ret, _len_ret);
		free(_in_ret);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_scan_collect_last_primary(void* pms)
{
	ms_ecall_scan_collect_last_primary_t* ms = SGX_CAST(ms_ecall_scan_collect_last_primary_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	uint8_t* _tmp_input_rows = ms->ms_input_rows;
	uint8_t* _tmp_output = ms->ms_output;
	uint32_t _tmp_output_length = ms->ms_output_length;
	size_t _len_output = _tmp_output_length;
	uint8_t* _in_output = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_scan_collect_last_primary_t));
	CHECK_UNIQUE_POINTER(_tmp_output, _len_output);

	if (_tmp_output != NULL) {
		if ((_in_output = (uint8_t*)malloc(_len_output)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_output, 0, _len_output);
	}
	ecall_scan_collect_last_primary(ms->ms_op_code, _tmp_input_rows, ms->ms_input_rows_length, ms->ms_num_rows, _in_output, _tmp_output_length);
err:
	if (_in_output) {
		memcpy(_tmp_output, _in_output, _len_output);
		free(_in_output);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_process_join_boundary(void* pms)
{
	ms_ecall_process_join_boundary_t* ms = SGX_CAST(ms_ecall_process_join_boundary_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	uint8_t* _tmp_input_rows = ms->ms_input_rows;
	uint8_t* _tmp_output_rows = ms->ms_output_rows;
	uint32_t _tmp_output_rows_size = ms->ms_output_rows_size;
	size_t _len_output_rows = _tmp_output_rows_size;
	uint8_t* _in_output_rows = NULL;
	uint8_t* _tmp_enc_table_p = ms->ms_enc_table_p;
	size_t _len_enc_table_p = ((_tmp_enc_table_p) ? enc_table_id_size(_tmp_enc_table_p) : 0);
	uint8_t* _in_enc_table_p = NULL;
	uint8_t* _tmp_enc_table_f = ms->ms_enc_table_f;
	size_t _len_enc_table_f = ((_tmp_enc_table_f) ? enc_table_id_size(_tmp_enc_table_f) : 0);
	uint8_t* _in_enc_table_f = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_process_join_boundary_t));
	CHECK_UNIQUE_POINTER(_tmp_output_rows, _len_output_rows);
	CHECK_UNIQUE_POINTER(_tmp_enc_table_p, _len_enc_table_p);
	CHECK_UNIQUE_POINTER(_tmp_enc_table_f, _len_enc_table_f);

	if (_tmp_output_rows != NULL) {
		if ((_in_output_rows = (uint8_t*)malloc(_len_output_rows)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_output_rows, 0, _len_output_rows);
	}
	if (_tmp_enc_table_p != NULL) {
		_in_enc_table_p = (uint8_t*)malloc(_len_enc_table_p);
		if (_in_enc_table_p == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_enc_table_p, _tmp_enc_table_p, _len_enc_table_p);

		/* check whether the pointer is modified. */
		if (enc_table_id_size(_in_enc_table_p) != _len_enc_table_p) {
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
	}
	if (_tmp_enc_table_f != NULL) {
		_in_enc_table_f = (uint8_t*)malloc(_len_enc_table_f);
		if (_in_enc_table_f == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_enc_table_f, _tmp_enc_table_f, _len_enc_table_f);

		/* check whether the pointer is modified. */
		if (enc_table_id_size(_in_enc_table_f) != _len_enc_table_f) {
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
	}
	ecall_process_join_boundary(_tmp_input_rows, ms->ms_input_rows_length, ms->ms_num_rows, _in_output_rows, _tmp_output_rows_size, _in_enc_table_p, _in_enc_table_f);
err:
	if (_in_output_rows) {
		memcpy(_tmp_output_rows, _in_output_rows, _len_output_rows);
		free(_in_output_rows);
	}
	if (_in_enc_table_p) free(_in_enc_table_p);
	if (_in_enc_table_f) free(_in_enc_table_f);

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_sort_merge_join(void* pms)
{
	ms_ecall_sort_merge_join_t* ms = SGX_CAST(ms_ecall_sort_merge_join_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	uint8_t* _tmp_input_rows = ms->ms_input_rows;
	uint8_t* _tmp_join_row = ms->ms_join_row;
	size_t _len_join_row = ((_tmp_join_row) ? join_row_size(_tmp_join_row) : 0);
	uint8_t* _in_join_row = NULL;
	uint8_t* _tmp_output_rows = ms->ms_output_rows;
	uint32_t _tmp_output_rows_length = ms->ms_output_rows_length;
	size_t _len_output_rows = _tmp_output_rows_length;
	uint8_t* _in_output_rows = NULL;
	uint32_t* _tmp_actual_output_length = ms->ms_actual_output_length;
	size_t _len_actual_output_length = 4;
	uint32_t* _in_actual_output_length = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_sort_merge_join_t));
	CHECK_UNIQUE_POINTER(_tmp_join_row, _len_join_row);
	CHECK_UNIQUE_POINTER(_tmp_output_rows, _len_output_rows);
	CHECK_UNIQUE_POINTER(_tmp_actual_output_length, _len_actual_output_length);

	if (_tmp_join_row != NULL) {
		_in_join_row = (uint8_t*)malloc(_len_join_row);
		if (_in_join_row == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_join_row, _tmp_join_row, _len_join_row);

		/* check whether the pointer is modified. */
		if (join_row_size(_in_join_row) != _len_join_row) {
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
	}
	if (_tmp_output_rows != NULL) {
		if ((_in_output_rows = (uint8_t*)malloc(_len_output_rows)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_output_rows, 0, _len_output_rows);
	}
	if (_tmp_actual_output_length != NULL) {
		if ((_in_actual_output_length = (uint32_t*)malloc(_len_actual_output_length)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_actual_output_length, 0, _len_actual_output_length);
	}
	ecall_sort_merge_join(ms->ms_op_code, _tmp_input_rows, ms->ms_input_rows_length, ms->ms_num_rows, _in_join_row, ms->ms_join_row_length, _in_output_rows, _tmp_output_rows_length, _in_actual_output_length);
err:
	if (_in_join_row) free(_in_join_row);
	if (_in_output_rows) {
		memcpy(_tmp_output_rows, _in_output_rows, _len_output_rows);
		free(_in_output_rows);
	}
	if (_in_actual_output_length) {
		memcpy(_tmp_actual_output_length, _in_actual_output_length, _len_actual_output_length);
		free(_in_actual_output_length);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_join_sort_preprocess(void* pms)
{
	ms_ecall_join_sort_preprocess_t* ms = SGX_CAST(ms_ecall_join_sort_preprocess_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	uint8_t* _tmp_table_id = ms->ms_table_id;
	size_t _len_table_id = ((_tmp_table_id) ? enc_table_id_size(_tmp_table_id) : 0);
	uint8_t* _in_table_id = NULL;
	uint8_t* _tmp_input_row = ms->ms_input_row;
	uint8_t* _tmp_output_row = ms->ms_output_row;
	uint32_t _tmp_output_row_len = ms->ms_output_row_len;
	size_t _len_output_row = _tmp_output_row_len;
	uint8_t* _in_output_row = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_join_sort_preprocess_t));
	CHECK_UNIQUE_POINTER(_tmp_table_id, _len_table_id);
	CHECK_UNIQUE_POINTER(_tmp_output_row, _len_output_row);

	if (_tmp_table_id != NULL) {
		_in_table_id = (uint8_t*)malloc(_len_table_id);
		if (_in_table_id == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_table_id, _tmp_table_id, _len_table_id);

		/* check whether the pointer is modified. */
		if (enc_table_id_size(_in_table_id) != _len_table_id) {
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
	}
	if (_tmp_output_row != NULL) {
		if ((_in_output_row = (uint8_t*)malloc(_len_output_row)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_output_row, 0, _len_output_row);
	}
	ecall_join_sort_preprocess(ms->ms_op_code, _in_table_id, _tmp_input_row, ms->ms_input_row_len, ms->ms_num_rows, _in_output_row, _tmp_output_row_len);
err:
	if (_in_table_id) free(_in_table_id);
	if (_in_output_row) {
		memcpy(_tmp_output_row, _in_output_row, _len_output_row);
		free(_in_output_row);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_type_char(void* pms)
{
	ms_ecall_type_char_t* ms = SGX_CAST(ms_ecall_type_char_t*, pms);
	sgx_status_t status = SGX_SUCCESS;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_type_char_t));

	ecall_type_char(ms->ms_val);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_type_int(void* pms)
{
	ms_ecall_type_int_t* ms = SGX_CAST(ms_ecall_type_int_t*, pms);
	sgx_status_t status = SGX_SUCCESS;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_type_int_t));

	ecall_type_int(ms->ms_val);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_type_float(void* pms)
{
	ms_ecall_type_float_t* ms = SGX_CAST(ms_ecall_type_float_t*, pms);
	sgx_status_t status = SGX_SUCCESS;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_type_float_t));

	ecall_type_float(ms->ms_val);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_type_double(void* pms)
{
	ms_ecall_type_double_t* ms = SGX_CAST(ms_ecall_type_double_t*, pms);
	sgx_status_t status = SGX_SUCCESS;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_type_double_t));

	ecall_type_double(ms->ms_val);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_type_size_t(void* pms)
{
	ms_ecall_type_size_t_t* ms = SGX_CAST(ms_ecall_type_size_t_t*, pms);
	sgx_status_t status = SGX_SUCCESS;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_type_size_t_t));

	ecall_type_size_t(ms->ms_val);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_type_wchar_t(void* pms)
{
	ms_ecall_type_wchar_t_t* ms = SGX_CAST(ms_ecall_type_wchar_t_t*, pms);
	sgx_status_t status = SGX_SUCCESS;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_type_wchar_t_t));

	ecall_type_wchar_t(ms->ms_val);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_type_struct(void* pms)
{
	ms_ecall_type_struct_t* ms = SGX_CAST(ms_ecall_type_struct_t*, pms);
	sgx_status_t status = SGX_SUCCESS;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_type_struct_t));

	ecall_type_struct(ms->ms_val);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_type_enum_union(void* pms)
{
	ms_ecall_type_enum_union_t* ms = SGX_CAST(ms_ecall_type_enum_union_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	union union_foo_t* _tmp_val2 = ms->ms_val2;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_type_enum_union_t));

	ecall_type_enum_union(ms->ms_val1, _tmp_val2);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_pointer_user_check(void* pms)
{
	ms_ecall_pointer_user_check_t* ms = SGX_CAST(ms_ecall_pointer_user_check_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	void* _tmp_val = ms->ms_val;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_pointer_user_check_t));

	ms->ms_retval = ecall_pointer_user_check(_tmp_val, ms->ms_sz);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_pointer_in(void* pms)
{
	ms_ecall_pointer_in_t* ms = SGX_CAST(ms_ecall_pointer_in_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	int* _tmp_val = ms->ms_val;
	size_t _len_val = sizeof(*_tmp_val);
	int* _in_val = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_pointer_in_t));
	CHECK_UNIQUE_POINTER(_tmp_val, _len_val);

	if (_tmp_val != NULL) {
		_in_val = (int*)malloc(_len_val);
		if (_in_val == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_val, _tmp_val, _len_val);
	}
	ecall_pointer_in(_in_val);
err:
	if (_in_val) free(_in_val);

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_pointer_out(void* pms)
{
	ms_ecall_pointer_out_t* ms = SGX_CAST(ms_ecall_pointer_out_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	int* _tmp_val = ms->ms_val;
	size_t _len_val = sizeof(*_tmp_val);
	int* _in_val = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_pointer_out_t));
	CHECK_UNIQUE_POINTER(_tmp_val, _len_val);

	if (_tmp_val != NULL) {
		if ((_in_val = (int*)malloc(_len_val)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_val, 0, _len_val);
	}
	ecall_pointer_out(_in_val);
err:
	if (_in_val) {
		memcpy(_tmp_val, _in_val, _len_val);
		free(_in_val);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_pointer_in_out(void* pms)
{
	ms_ecall_pointer_in_out_t* ms = SGX_CAST(ms_ecall_pointer_in_out_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	int* _tmp_val = ms->ms_val;
	size_t _len_val = sizeof(*_tmp_val);
	int* _in_val = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_pointer_in_out_t));
	CHECK_UNIQUE_POINTER(_tmp_val, _len_val);

	if (_tmp_val != NULL) {
		_in_val = (int*)malloc(_len_val);
		if (_in_val == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_val, _tmp_val, _len_val);
	}
	ecall_pointer_in_out(_in_val);
err:
	if (_in_val) {
		memcpy(_tmp_val, _in_val, _len_val);
		free(_in_val);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_pointer_string(void* pms)
{
	ms_ecall_pointer_string_t* ms = SGX_CAST(ms_ecall_pointer_string_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	char* _tmp_str = ms->ms_str;
	size_t _len_str = _tmp_str ? strlen(_tmp_str) + 1 : 0;
	char* _in_str = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_pointer_string_t));
	CHECK_UNIQUE_POINTER(_tmp_str, _len_str);

	if (_tmp_str != NULL) {
		_in_str = (char*)malloc(_len_str);
		if (_in_str == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_str, _tmp_str, _len_str);
		_in_str[_len_str - 1] = '\0';
	}
	ecall_pointer_string(_in_str);
err:
	if (_in_str) {
		memcpy(_tmp_str, _in_str, _len_str);
		free(_in_str);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_pointer_string_const(void* pms)
{
	ms_ecall_pointer_string_const_t* ms = SGX_CAST(ms_ecall_pointer_string_const_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	char* _tmp_str = ms->ms_str;
	size_t _len_str = _tmp_str ? strlen(_tmp_str) + 1 : 0;
	char* _in_str = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_pointer_string_const_t));
	CHECK_UNIQUE_POINTER(_tmp_str, _len_str);

	if (_tmp_str != NULL) {
		_in_str = (char*)malloc(_len_str);
		if (_in_str == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy((void*)_in_str, _tmp_str, _len_str);
		_in_str[_len_str - 1] = '\0';
	}
	ecall_pointer_string_const((const char*)_in_str);
err:
	if (_in_str) free((void*)_in_str);

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_pointer_size(void* pms)
{
	ms_ecall_pointer_size_t* ms = SGX_CAST(ms_ecall_pointer_size_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	void* _tmp_ptr = ms->ms_ptr;
	size_t _tmp_len = ms->ms_len;
	size_t _len_ptr = _tmp_len;
	void* _in_ptr = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_pointer_size_t));
	CHECK_UNIQUE_POINTER(_tmp_ptr, _len_ptr);

	if (_tmp_ptr != NULL) {
		_in_ptr = (void*)malloc(_len_ptr);
		if (_in_ptr == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_ptr, _tmp_ptr, _len_ptr);
	}
	ecall_pointer_size(_in_ptr, _tmp_len);
err:
	if (_in_ptr) {
		memcpy(_tmp_ptr, _in_ptr, _len_ptr);
		free(_in_ptr);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_pointer_count(void* pms)
{
	ms_ecall_pointer_count_t* ms = SGX_CAST(ms_ecall_pointer_count_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	int* _tmp_arr = ms->ms_arr;
	int _tmp_cnt = ms->ms_cnt;
	size_t _len_arr = _tmp_cnt * sizeof(*_tmp_arr);
	int* _in_arr = NULL;

	if ((size_t)_tmp_cnt > (SIZE_MAX / sizeof(*_tmp_arr))) {
		status = SGX_ERROR_INVALID_PARAMETER;
		goto err;
	}

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_pointer_count_t));
	CHECK_UNIQUE_POINTER(_tmp_arr, _len_arr);

	if (_tmp_arr != NULL) {
		_in_arr = (int*)malloc(_len_arr);
		if (_in_arr == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_arr, _tmp_arr, _len_arr);
	}
	ecall_pointer_count(_in_arr, _tmp_cnt);
err:
	if (_in_arr) {
		memcpy(_tmp_arr, _in_arr, _len_arr);
		free(_in_arr);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_pointer_isptr_readonly(void* pms)
{
	ms_ecall_pointer_isptr_readonly_t* ms = SGX_CAST(ms_ecall_pointer_isptr_readonly_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	buffer_t _tmp_buf = ms->ms_buf;
	size_t _tmp_len = ms->ms_len;
	size_t _len_buf = _tmp_len;
	buffer_t _in_buf = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_pointer_isptr_readonly_t));
	CHECK_UNIQUE_POINTER(_tmp_buf, _len_buf);

	if (_tmp_buf != NULL) {
		_in_buf = (buffer_t)malloc(_len_buf);
		if (_in_buf == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy((void*)_in_buf, _tmp_buf, _len_buf);
	}
	ecall_pointer_isptr_readonly(_in_buf, _tmp_len);
err:
	if (_in_buf) free((void*)_in_buf);

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_pointer_sizefunc(void* pms)
{
	ms_ecall_pointer_sizefunc_t* ms = SGX_CAST(ms_ecall_pointer_sizefunc_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	char* _tmp_buf = ms->ms_buf;
	size_t _len_buf = ((_tmp_buf) ? get_buffer_len(_tmp_buf) : 0);
	char* _in_buf = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_pointer_sizefunc_t));
	CHECK_UNIQUE_POINTER(_tmp_buf, _len_buf);

	if (_tmp_buf != NULL) {
		_in_buf = (char*)malloc(_len_buf);
		if (_in_buf == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_buf, _tmp_buf, _len_buf);

		/* check whether the pointer is modified. */
		if (get_buffer_len(_in_buf) != _len_buf) {
			status = SGX_ERROR_INVALID_PARAMETER;
			goto err;
		}
	}
	ecall_pointer_sizefunc(_in_buf);
err:
	if (_in_buf) {
		memcpy(_tmp_buf, _in_buf, _len_buf);
		free(_in_buf);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ocall_pointer_attr(void* pms)
{
	sgx_status_t status = SGX_SUCCESS;
	if (pms != NULL) return SGX_ERROR_INVALID_PARAMETER;
	ocall_pointer_attr();
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_array_user_check(void* pms)
{
	ms_ecall_array_user_check_t* ms = SGX_CAST(ms_ecall_array_user_check_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	int* _tmp_arr = ms->ms_arr;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_array_user_check_t));

	ecall_array_user_check(_tmp_arr);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_array_in(void* pms)
{
	ms_ecall_array_in_t* ms = SGX_CAST(ms_ecall_array_in_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	int* _tmp_arr = ms->ms_arr;
	size_t _len_arr = 4 * sizeof(*_tmp_arr);
	int* _in_arr = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_array_in_t));
	CHECK_UNIQUE_POINTER(_tmp_arr, _len_arr);

	if (_tmp_arr != NULL) {
		_in_arr = (int*)malloc(_len_arr);
		if (_in_arr == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_arr, _tmp_arr, _len_arr);
	}
	ecall_array_in(_in_arr);
err:
	if (_in_arr) free(_in_arr);

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_array_out(void* pms)
{
	ms_ecall_array_out_t* ms = SGX_CAST(ms_ecall_array_out_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	int* _tmp_arr = ms->ms_arr;
	size_t _len_arr = 4 * sizeof(*_tmp_arr);
	int* _in_arr = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_array_out_t));
	CHECK_UNIQUE_POINTER(_tmp_arr, _len_arr);

	if (_tmp_arr != NULL) {
		if ((_in_arr = (int*)malloc(_len_arr)) == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memset((void*)_in_arr, 0, _len_arr);
	}
	ecall_array_out(_in_arr);
err:
	if (_in_arr) {
		memcpy(_tmp_arr, _in_arr, _len_arr);
		free(_in_arr);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_array_in_out(void* pms)
{
	ms_ecall_array_in_out_t* ms = SGX_CAST(ms_ecall_array_in_out_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	int* _tmp_arr = ms->ms_arr;
	size_t _len_arr = 4 * sizeof(*_tmp_arr);
	int* _in_arr = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_array_in_out_t));
	CHECK_UNIQUE_POINTER(_tmp_arr, _len_arr);

	if (_tmp_arr != NULL) {
		_in_arr = (int*)malloc(_len_arr);
		if (_in_arr == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_arr, _tmp_arr, _len_arr);
	}
	ecall_array_in_out(_in_arr);
err:
	if (_in_arr) {
		memcpy(_tmp_arr, _in_arr, _len_arr);
		free(_in_arr);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_array_isary(void* pms)
{
	ms_ecall_array_isary_t* ms = SGX_CAST(ms_ecall_array_isary_t*, pms);
	sgx_status_t status = SGX_SUCCESS;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_array_isary_t));

	ecall_array_isary((ms->ms_arr != NULL) ? (*ms->ms_arr) : NULL);


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_function_calling_convs(void* pms)
{
	sgx_status_t status = SGX_SUCCESS;
	if (pms != NULL) return SGX_ERROR_INVALID_PARAMETER;
	ecall_function_calling_convs();
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_function_public(void* pms)
{
	sgx_status_t status = SGX_SUCCESS;
	if (pms != NULL) return SGX_ERROR_INVALID_PARAMETER;
	ecall_function_public();
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_function_private(void* pms)
{
	ms_ecall_function_private_t* ms = SGX_CAST(ms_ecall_function_private_t*, pms);
	sgx_status_t status = SGX_SUCCESS;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_function_private_t));

	ms->ms_retval = ecall_function_private();


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_malloc_free(void* pms)
{
	sgx_status_t status = SGX_SUCCESS;
	if (pms != NULL) return SGX_ERROR_INVALID_PARAMETER;
	ecall_malloc_free();
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_sgx_cpuid(void* pms)
{
	ms_ecall_sgx_cpuid_t* ms = SGX_CAST(ms_ecall_sgx_cpuid_t*, pms);
	sgx_status_t status = SGX_SUCCESS;
	int* _tmp_cpuinfo = ms->ms_cpuinfo;
	size_t _len_cpuinfo = 4 * sizeof(*_tmp_cpuinfo);
	int* _in_cpuinfo = NULL;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_sgx_cpuid_t));
	CHECK_UNIQUE_POINTER(_tmp_cpuinfo, _len_cpuinfo);

	if (_tmp_cpuinfo != NULL) {
		_in_cpuinfo = (int*)malloc(_len_cpuinfo);
		if (_in_cpuinfo == NULL) {
			status = SGX_ERROR_OUT_OF_MEMORY;
			goto err;
		}

		memcpy(_in_cpuinfo, _tmp_cpuinfo, _len_cpuinfo);
	}
	ecall_sgx_cpuid(_in_cpuinfo, ms->ms_leaf);
err:
	if (_in_cpuinfo) {
		memcpy(_tmp_cpuinfo, _in_cpuinfo, _len_cpuinfo);
		free(_in_cpuinfo);
	}

	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_exception(void* pms)
{
	sgx_status_t status = SGX_SUCCESS;
	if (pms != NULL) return SGX_ERROR_INVALID_PARAMETER;
	ecall_exception();
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_map(void* pms)
{
	sgx_status_t status = SGX_SUCCESS;
	if (pms != NULL) return SGX_ERROR_INVALID_PARAMETER;
	ecall_map();
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_increase_counter(void* pms)
{
	ms_ecall_increase_counter_t* ms = SGX_CAST(ms_ecall_increase_counter_t*, pms);
	sgx_status_t status = SGX_SUCCESS;

	CHECK_REF_POINTER(pms, sizeof(ms_ecall_increase_counter_t));

	ms->ms_retval = ecall_increase_counter();


	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_producer(void* pms)
{
	sgx_status_t status = SGX_SUCCESS;
	if (pms != NULL) return SGX_ERROR_INVALID_PARAMETER;
	ecall_producer();
	return status;
}

static sgx_status_t SGX_CDECL sgx_ecall_consumer(void* pms)
{
	sgx_status_t status = SGX_SUCCESS;
	if (pms != NULL) return SGX_ERROR_INVALID_PARAMETER;
	ecall_consumer();
	return status;
}

SGX_EXTERNC const struct {
	size_t nr_ecall;
	struct {void* ecall_addr; uint8_t is_priv;} ecall_table[49];
} g_ecall_table = {
	49,
	{
		{(void*)(uintptr_t)sgx_ecall_filter_single_row, 0},
		{(void*)(uintptr_t)sgx_ecall_encrypt, 0},
		{(void*)(uintptr_t)sgx_ecall_decrypt, 0},
		{(void*)(uintptr_t)sgx_ecall_test_int, 0},
		{(void*)(uintptr_t)sgx_ecall_oblivious_sort_int, 0},
		{(void*)(uintptr_t)sgx_ecall_oblivious_sort, 0},
		{(void*)(uintptr_t)sgx_ecall_random_id, 0},
		{(void*)(uintptr_t)sgx_ecall_scan_aggregation_count_distinct, 0},
		{(void*)(uintptr_t)sgx_ecall_test, 0},
		{(void*)(uintptr_t)sgx_ecall_process_boundary_records, 0},
		{(void*)(uintptr_t)sgx_ecall_final_aggregation, 0},
		{(void*)(uintptr_t)sgx_ecall_scan_collect_last_primary, 0},
		{(void*)(uintptr_t)sgx_ecall_process_join_boundary, 0},
		{(void*)(uintptr_t)sgx_ecall_sort_merge_join, 0},
		{(void*)(uintptr_t)sgx_ecall_join_sort_preprocess, 0},
		{(void*)(uintptr_t)sgx_ecall_type_char, 0},
		{(void*)(uintptr_t)sgx_ecall_type_int, 0},
		{(void*)(uintptr_t)sgx_ecall_type_float, 0},
		{(void*)(uintptr_t)sgx_ecall_type_double, 0},
		{(void*)(uintptr_t)sgx_ecall_type_size_t, 0},
		{(void*)(uintptr_t)sgx_ecall_type_wchar_t, 0},
		{(void*)(uintptr_t)sgx_ecall_type_struct, 0},
		{(void*)(uintptr_t)sgx_ecall_type_enum_union, 0},
		{(void*)(uintptr_t)sgx_ecall_pointer_user_check, 0},
		{(void*)(uintptr_t)sgx_ecall_pointer_in, 0},
		{(void*)(uintptr_t)sgx_ecall_pointer_out, 0},
		{(void*)(uintptr_t)sgx_ecall_pointer_in_out, 0},
		{(void*)(uintptr_t)sgx_ecall_pointer_string, 0},
		{(void*)(uintptr_t)sgx_ecall_pointer_string_const, 0},
		{(void*)(uintptr_t)sgx_ecall_pointer_size, 0},
		{(void*)(uintptr_t)sgx_ecall_pointer_count, 0},
		{(void*)(uintptr_t)sgx_ecall_pointer_isptr_readonly, 0},
		{(void*)(uintptr_t)sgx_ecall_pointer_sizefunc, 0},
		{(void*)(uintptr_t)sgx_ocall_pointer_attr, 0},
		{(void*)(uintptr_t)sgx_ecall_array_user_check, 0},
		{(void*)(uintptr_t)sgx_ecall_array_in, 0},
		{(void*)(uintptr_t)sgx_ecall_array_out, 0},
		{(void*)(uintptr_t)sgx_ecall_array_in_out, 0},
		{(void*)(uintptr_t)sgx_ecall_array_isary, 0},
		{(void*)(uintptr_t)sgx_ecall_function_calling_convs, 0},
		{(void*)(uintptr_t)sgx_ecall_function_public, 0},
		{(void*)(uintptr_t)sgx_ecall_function_private, 1},
		{(void*)(uintptr_t)sgx_ecall_malloc_free, 0},
		{(void*)(uintptr_t)sgx_ecall_sgx_cpuid, 0},
		{(void*)(uintptr_t)sgx_ecall_exception, 0},
		{(void*)(uintptr_t)sgx_ecall_map, 0},
		{(void*)(uintptr_t)sgx_ecall_increase_counter, 0},
		{(void*)(uintptr_t)sgx_ecall_producer, 0},
		{(void*)(uintptr_t)sgx_ecall_consumer, 0},
	}
};

SGX_EXTERNC const struct {
	size_t nr_ocall;
	uint8_t entry_table[12][49];
} g_dyn_entry_table = {
	12,
	{
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
		{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, },
	}
};


sgx_status_t SGX_CDECL ocall_print_string(const char* str)
{
	sgx_status_t status = SGX_SUCCESS;
	size_t _len_str = str ? strlen(str) + 1 : 0;

	ms_ocall_print_string_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_ocall_print_string_t);
	void *__tmp = NULL;

	ocalloc_size += (str != NULL && sgx_is_within_enclave(str, _len_str)) ? _len_str : 0;

	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_ocall_print_string_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_ocall_print_string_t));

	if (str != NULL && sgx_is_within_enclave(str, _len_str)) {
		ms->ms_str = (char*)__tmp;
		__tmp = (void *)((size_t)__tmp + _len_str);
		memcpy((void*)ms->ms_str, str, _len_str);
	} else if (str == NULL) {
		ms->ms_str = NULL;
	} else {
		sgx_ocfree();
		return SGX_ERROR_INVALID_PARAMETER;
	}
	
	status = sgx_ocall(0, ms);


	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL ocall_pointer_user_check(int* val)
{
	sgx_status_t status = SGX_SUCCESS;

	ms_ocall_pointer_user_check_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_ocall_pointer_user_check_t);
	void *__tmp = NULL;


	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_ocall_pointer_user_check_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_ocall_pointer_user_check_t));

	ms->ms_val = SGX_CAST(int*, val);
	status = sgx_ocall(1, ms);


	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL ocall_pointer_in(int* val)
{
	sgx_status_t status = SGX_SUCCESS;
	size_t _len_val = sizeof(*val);

	ms_ocall_pointer_in_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_ocall_pointer_in_t);
	void *__tmp = NULL;

	ocalloc_size += (val != NULL && sgx_is_within_enclave(val, _len_val)) ? _len_val : 0;

	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_ocall_pointer_in_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_ocall_pointer_in_t));

	if (val != NULL && sgx_is_within_enclave(val, _len_val)) {
		ms->ms_val = (int*)__tmp;
		__tmp = (void *)((size_t)__tmp + _len_val);
		memcpy(ms->ms_val, val, _len_val);
	} else if (val == NULL) {
		ms->ms_val = NULL;
	} else {
		sgx_ocfree();
		return SGX_ERROR_INVALID_PARAMETER;
	}
	
	status = sgx_ocall(2, ms);


	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL ocall_pointer_out(int* val)
{
	sgx_status_t status = SGX_SUCCESS;
	size_t _len_val = sizeof(*val);

	ms_ocall_pointer_out_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_ocall_pointer_out_t);
	void *__tmp = NULL;

	ocalloc_size += (val != NULL && sgx_is_within_enclave(val, _len_val)) ? _len_val : 0;

	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_ocall_pointer_out_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_ocall_pointer_out_t));

	if (val != NULL && sgx_is_within_enclave(val, _len_val)) {
		ms->ms_val = (int*)__tmp;
		__tmp = (void *)((size_t)__tmp + _len_val);
		memset(ms->ms_val, 0, _len_val);
	} else if (val == NULL) {
		ms->ms_val = NULL;
	} else {
		sgx_ocfree();
		return SGX_ERROR_INVALID_PARAMETER;
	}
	
	status = sgx_ocall(3, ms);

	if (val) memcpy((void*)val, ms->ms_val, _len_val);

	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL ocall_pointer_in_out(int* val)
{
	sgx_status_t status = SGX_SUCCESS;
	size_t _len_val = sizeof(*val);

	ms_ocall_pointer_in_out_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_ocall_pointer_in_out_t);
	void *__tmp = NULL;

	ocalloc_size += (val != NULL && sgx_is_within_enclave(val, _len_val)) ? _len_val : 0;

	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_ocall_pointer_in_out_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_ocall_pointer_in_out_t));

	if (val != NULL && sgx_is_within_enclave(val, _len_val)) {
		ms->ms_val = (int*)__tmp;
		__tmp = (void *)((size_t)__tmp + _len_val);
		memcpy(ms->ms_val, val, _len_val);
	} else if (val == NULL) {
		ms->ms_val = NULL;
	} else {
		sgx_ocfree();
		return SGX_ERROR_INVALID_PARAMETER;
	}
	
	status = sgx_ocall(4, ms);

	if (val) memcpy((void*)val, ms->ms_val, _len_val);

	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL memccpy(void** retval, void* dest, const void* src, int val, size_t len)
{
	sgx_status_t status = SGX_SUCCESS;
	size_t _len_dest = len;
	size_t _len_src = len;

	ms_memccpy_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_memccpy_t);
	void *__tmp = NULL;

	ocalloc_size += (dest != NULL && sgx_is_within_enclave(dest, _len_dest)) ? _len_dest : 0;
	ocalloc_size += (src != NULL && sgx_is_within_enclave(src, _len_src)) ? _len_src : 0;

	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_memccpy_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_memccpy_t));

	if (dest != NULL && sgx_is_within_enclave(dest, _len_dest)) {
		ms->ms_dest = (void*)__tmp;
		__tmp = (void *)((size_t)__tmp + _len_dest);
		memcpy(ms->ms_dest, dest, _len_dest);
	} else if (dest == NULL) {
		ms->ms_dest = NULL;
	} else {
		sgx_ocfree();
		return SGX_ERROR_INVALID_PARAMETER;
	}
	
	if (src != NULL && sgx_is_within_enclave(src, _len_src)) {
		ms->ms_src = (void*)__tmp;
		__tmp = (void *)((size_t)__tmp + _len_src);
		memcpy((void*)ms->ms_src, src, _len_src);
	} else if (src == NULL) {
		ms->ms_src = NULL;
	} else {
		sgx_ocfree();
		return SGX_ERROR_INVALID_PARAMETER;
	}
	
	ms->ms_val = val;
	ms->ms_len = len;
	status = sgx_ocall(5, ms);

	if (retval) *retval = ms->ms_retval;
	if (dest) memcpy((void*)dest, ms->ms_dest, _len_dest);

	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL ocall_function_allow()
{
	sgx_status_t status = SGX_SUCCESS;
	status = sgx_ocall(6, NULL);

	return status;
}

sgx_status_t SGX_CDECL sgx_oc_cpuidex(int cpuinfo[4], int leaf, int subleaf)
{
	sgx_status_t status = SGX_SUCCESS;
	size_t _len_cpuinfo = 4 * sizeof(*cpuinfo);

	ms_sgx_oc_cpuidex_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_sgx_oc_cpuidex_t);
	void *__tmp = NULL;

	ocalloc_size += (cpuinfo != NULL && sgx_is_within_enclave(cpuinfo, _len_cpuinfo)) ? _len_cpuinfo : 0;

	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_sgx_oc_cpuidex_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_sgx_oc_cpuidex_t));

	if (cpuinfo != NULL && sgx_is_within_enclave(cpuinfo, _len_cpuinfo)) {
		ms->ms_cpuinfo = (int*)__tmp;
		__tmp = (void *)((size_t)__tmp + _len_cpuinfo);
		memcpy(ms->ms_cpuinfo, cpuinfo, _len_cpuinfo);
	} else if (cpuinfo == NULL) {
		ms->ms_cpuinfo = NULL;
	} else {
		sgx_ocfree();
		return SGX_ERROR_INVALID_PARAMETER;
	}
	
	ms->ms_leaf = leaf;
	ms->ms_subleaf = subleaf;
	status = sgx_ocall(7, ms);

	if (cpuinfo) memcpy((void*)cpuinfo, ms->ms_cpuinfo, _len_cpuinfo);

	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL sgx_thread_wait_untrusted_event_ocall(int* retval, const void* self)
{
	sgx_status_t status = SGX_SUCCESS;

	ms_sgx_thread_wait_untrusted_event_ocall_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_sgx_thread_wait_untrusted_event_ocall_t);
	void *__tmp = NULL;


	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_sgx_thread_wait_untrusted_event_ocall_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_sgx_thread_wait_untrusted_event_ocall_t));

	ms->ms_self = SGX_CAST(void*, self);
	status = sgx_ocall(8, ms);

	if (retval) *retval = ms->ms_retval;

	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL sgx_thread_set_untrusted_event_ocall(int* retval, const void* waiter)
{
	sgx_status_t status = SGX_SUCCESS;

	ms_sgx_thread_set_untrusted_event_ocall_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_sgx_thread_set_untrusted_event_ocall_t);
	void *__tmp = NULL;


	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_sgx_thread_set_untrusted_event_ocall_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_sgx_thread_set_untrusted_event_ocall_t));

	ms->ms_waiter = SGX_CAST(void*, waiter);
	status = sgx_ocall(9, ms);

	if (retval) *retval = ms->ms_retval;

	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL sgx_thread_setwait_untrusted_events_ocall(int* retval, const void* waiter, const void* self)
{
	sgx_status_t status = SGX_SUCCESS;

	ms_sgx_thread_setwait_untrusted_events_ocall_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_sgx_thread_setwait_untrusted_events_ocall_t);
	void *__tmp = NULL;


	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_sgx_thread_setwait_untrusted_events_ocall_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_sgx_thread_setwait_untrusted_events_ocall_t));

	ms->ms_waiter = SGX_CAST(void*, waiter);
	ms->ms_self = SGX_CAST(void*, self);
	status = sgx_ocall(10, ms);

	if (retval) *retval = ms->ms_retval;

	sgx_ocfree();
	return status;
}

sgx_status_t SGX_CDECL sgx_thread_set_multiple_untrusted_events_ocall(int* retval, const void** waiters, size_t total)
{
	sgx_status_t status = SGX_SUCCESS;
	size_t _len_waiters = total * sizeof(*waiters);

	ms_sgx_thread_set_multiple_untrusted_events_ocall_t* ms = NULL;
	size_t ocalloc_size = sizeof(ms_sgx_thread_set_multiple_untrusted_events_ocall_t);
	void *__tmp = NULL;

	ocalloc_size += (waiters != NULL && sgx_is_within_enclave(waiters, _len_waiters)) ? _len_waiters : 0;

	__tmp = sgx_ocalloc(ocalloc_size);
	if (__tmp == NULL) {
		sgx_ocfree();
		return SGX_ERROR_UNEXPECTED;
	}
	ms = (ms_sgx_thread_set_multiple_untrusted_events_ocall_t*)__tmp;
	__tmp = (void *)((size_t)__tmp + sizeof(ms_sgx_thread_set_multiple_untrusted_events_ocall_t));

	if (waiters != NULL && sgx_is_within_enclave(waiters, _len_waiters)) {
		ms->ms_waiters = (void**)__tmp;
		__tmp = (void *)((size_t)__tmp + _len_waiters);
		memcpy((void*)ms->ms_waiters, waiters, _len_waiters);
	} else if (waiters == NULL) {
		ms->ms_waiters = NULL;
	} else {
		sgx_ocfree();
		return SGX_ERROR_INVALID_PARAMETER;
	}
	
	ms->ms_total = total;
	status = sgx_ocall(11, ms);

	if (retval) *retval = ms->ms_retval;

	sgx_ocfree();
	return status;
}

