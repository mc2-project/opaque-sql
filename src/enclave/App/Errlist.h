#include <openenclave/host.h>
#include <string>

typedef struct _oe_errlist_t {
  oe_result_t err;
  const char *msg;
  const char *sug; /* Suggestion */
} oe_errlist_t;

/* Error codes defined by oe_result_t */
static oe_errlist_t oe_errlist[] = {
    {
      OE_FAILURE,
      "The function failed (without a more specific error code)",
      NULL
    },
    {
      OE_BUFFER_TOO_SMALL,
      "One or more output buffer function parameters is too small.",
      NULL
    },
    {
      OE_INVALID_PARAMETER,
      "The function failed (without a more specific error code)",
      NULL
    },
    {
      OE_REENTRANT_ECALL,
      "One or more output buffer function parameters is too small.",
      NULL
    },
    {
      OE_OUT_OF_MEMORY,
      "The function is out of memory. This usually occurs when **malloc** or a related function returns null.",
      NULL
    },
    {
      OE_OUT_OF_THREADS,
      "The function is unable to bind the current host thread to an enclave thread. This occurs when the host performs an **ECALL** while all enclave threads are in use.",
      NULL
    },
    {
      OE_UNEXPECTED,
      "The function encountered an unexpected failure.",
      NULL
    },
    {
      OE_VERIFY_FAILED,
      "A cryptographic verification failed. Examples include: \n- enclave quote verification\n-public key signature verification\n- certificate chain verification",
      NULL
    },
    {
      OE_NOT_FOUND,
      "The function failed to find a resource. Examples of resources include files, directories, and functions (ECALL/OCALL), container elements.",
      NULL
    },
    {
      OE_INTEGER_OVERFLOW,
      "The function encountered an overflow in an integer operation, which can occur in arithmetic operations and cast operations.",
      NULL
    },
    {
      OE_PUBLIC_KEY_NOT_FOUND,
      "The certificate does not contain a public key.",
      NULL
    },
    {
      OE_OUT_OF_BOUNDS,
      "An integer index is outside the expected range. For example, an array index is greater than or equal to the array size.",
      NULL
    },
    {
      OE_OVERLAPPED_COPY,
      "The function prevented an attempt to perform an overlapped copy, where the source and destination buffers are overlapping.",
      NULL
    },
    {
      OE_CONSTRAINT_FAILED,
     "The function detected a constraint failure. A constraint restricts the value of a field, parameter, or variable. For example, the value of **day_of_the_week** must be between 1 and 7 inclusive.",
      NULL
    },
    {
      OE_IOCTL_FAILED,
      "An **IOCTL** operation failed. Open Enclave uses **IOCTL** operations to communicate with the Intel SGX driver.",
      NULL
    },
    {
      OE_UNSUPPORTED,
      "The given operation is unsupported, usually by a particular platform or environment.",
      NULL
    },
    {
      OE_READ_FAILED,
      "The function failed to read data from a device (such as a socket, or file).",
      NULL
    },
    {
      OE_SERVICE_UNAVAILABLE,
      "A software service is unavailable (such as the AESM service).",
      NULL
    },
    {
      OE_ENCLAVE_ABORTING,
      "The operation cannot be completed because the enclave is aborting.",
      NULL
    },
    {
      OE_ENCLAVE_ABORTED,
      "The operation cannot be completed because the enclave has already aborted.",
      NULL
    },
    {
      OE_PLATFORM_ERROR,
      "The underlying platform or hardware returned an error. For example, an SGX user-mode instruction failed.",
      NULL
    },
    {
      OE_INVALID_CPUSVN,
      "The given **CPUSVN** value is invalid. An SGX user-mode instruction may return this error.",
      NULL
    },
    {
      OE_INVALID_ISVSVN,
      "The given **ISVSNV** value is invalid. An SGX user-mode instruction may return this error.",
      NULL
    },
    {
      OE_INVALID_KEYNAME,
      "The given **key name** is invalid. An SGX user-mode instruction may return this error.",
      NULL
    },
    {
      OE_DEBUG_DOWNGRADE,
      "Attempted to create a debug enclave with an enclave image that does not allow it.",
      NULL
    },
    {
      OE_REPORT_PARSE_ERROR,
      "Failed to parse an enclave report.",
      NULL
    },
    {
      OE_MISSING_CERTIFICATE_CHAIN,
      "The certificate chain is not available or missing.",
      NULL
    },
    {
      OE_BUSY,
      "An operation cannot be performed beause the resource is busy. For example, a non-recursive mutex cannot be locked because it is already locked.",
      NULL
    },
    {
      OE_NOT_OWNER,
      "An operation cannot be performed because the requestor is not the owner of the resource. For example, a thread cannot lock a mutex because it is not the thread that acquired the mutex.",
      NULL
    },
    {
      OE_INVALID_SGX_CERTIFICATE_EXTENSIONS,
      "The certificate does not contain the expected SGX extensions.",
      NULL
    },
    {
      OE_MEMORY_LEAK,
      "A memory leak was detected during enclave termination.",
      NULL
    },
    {
      OE_BAD_ALIGNMENT,
      "The data is improperly aligned for the given operation. This may occur when an output buffer parameter is not suitably aligned for the data it will receive.",
      NULL
    },
    {
      OE_JSON_INFO_PARSE_ERROR,
      "Failed to parse the trusted computing base (TCB) revocation data or the QE Identity data for the enclave.",
      NULL
    },
    {
      OE_TCB_LEVEL_INVALID,
      "The level of the trusted computing base (TCB) is not up to date for report verification.",
      NULL
    },
    {
      OE_QUOTE_PROVIDER_LOAD_ERROR,
      "Failed to load the quote provider library used for quote generation and attestation.",
      NULL
    },
    {
      OE_QUOTE_PROVIDER_CALL_ERROR,
      "A call to the quote provider failed.",
      NULL
    },
    {
      OE_INVALID_REVOCATION_INFO,
      "The certificate revocation data for attesting the trusted computing base (TCB) is invalid for this enclave.",
      NULL
    },
    {
      OE_INVALID_UTC_DATE_TIME,
      "The given UTC date-time string or structure is invalid. This occurs when (1) an element is out of range (year, month, day, hours, minutes, seconds), or (2) the UTC date-time string is malformed.",
      NULL
    },
    {
      OE_INVALID_QE_IDENTITY_INFO,
      "The QE identity data is invalid.",
      NULL
    },
    {
      OE_UNSUPPORTED_ENCLAVE_IMAGE,
      "The enclave image contains unsupported constructs.",
      NULL
    }
};
