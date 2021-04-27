import sys
import grpc
import atexit

import rpc_pb2
import rpc_pb2_grpc

from ctypes import cdll, c_uint8, c_size_t, byref, POINTER, addressof, c_char, c_char_p, c_wchar_p
lib = cdll.LoadLibrary('/home/opaque/opaque/src/enclave/ServiceProvider/build/libra_jni.so')
sp = lib.sp_new()

with open("user1.crt", "rb") as file:
  cert = file.read()

  cert_bytes = bytearray(cert)
  cert_len = c_size_t(len(cert_bytes))

  c_cert = c_uint8 * cert_len.value
  c_cert_bytes = c_cert.from_buffer(cert_bytes)

  lib.sp_init_wrapper(sp, c_cert_bytes, cert_len)

def perform_ra(stub):

    response = stub.relayGenerateReport(rpc_pb2.RARequest(name = "user"))
    key_request = rpc_pb2.KeyRequest(name = "user", success = True)

    # Parse and process
    try:
      # Parse and remove first item (which is list of eids) and last item (which is filler tail)
      splitter = "2d2d2d2d2d424547494e205055424c4943204b45592d2d2d2d2d" # hex of '-----BEGIN PUBLIC KEY-----'
      parsed =  response.report.split(splitter)

      eids = parsed[0].split()
      print(eids)
      reports = [(splitter + x) for x in parsed[1:][:-1]]

      if len(eids) != len(reports):
        raise Exception("number of enclaves and reports don't match")

      num_enclaves = len(eids)

      for i in range(num_enclaves):

        report_bytes = bytearray.fromhex(reports[i])
        report_len = c_size_t(len(report_bytes))

        c_report = c_uint8 * report_len.value
        c_report_bytes = c_report.from_buffer(report_bytes)

        ret_val = POINTER(c_uint8)()
        ret_len = c_size_t()

        f_process = lib.sp_process_enclave_report
        f_process(sp, c_report_bytes, byref(report_len), byref(ret_val), byref(ret_len))

        msg_carray = c_uint8 * ret_len.value
        msg_carray_bytes = msg_carray.from_address(addressof(ret_val.contents))

        # Copy ServiceProvider response into bytes
        msg = bytes(msg_carray_bytes)

        key_request.key.append(msg)
        key_request.eid.append(eids[i])

        response = stub.relayFinishAttestation(key_request)

        if response.success:
          print("Attestation successful")
        else:
          print("Attestation failed")

        f_free = lib.sp_free_array
        f_free(sp, byref(ret_val))

    except Exception as e:
      print("Report verification fail: " + str(e))

def decrypt(cipher):

    print(cipher)

    cipher_bytes = bytes(cipher, 'utf-8')
    cipher_len = c_size_t(len(cipher_bytes))
    c_cipher_bytes = c_char_p(cipher_bytes)

    ret_val = POINTER(c_uint8)()
    ret_len = c_size_t()

    f_process = lib.sp_decrypt
    f_process(sp, c_cipher_bytes, byref(cipher_len), byref(ret_val), byref(ret_len))

    msg_carray = c_uint8 * ret_len.value
    msg_carray_bytes = msg_carray.from_address(addressof(ret_val.contents))

    # Copy into bytes
    decrypted_bytes = bytes(msg_carray_bytes)
    f_free = lib.sp_free_array
    f_free(sp, byref(ret_val))

    return decrypted_bytes

def send_query(stub, query):
    response = stub.relayQuery(rpc_pb2.QueryRequest(query=query))
    if "postVerifyAndPrint" in query.split("(")[0]:
      parsed = response.data.splitlines()[:-1]
      for enc in parsed:
        try:
          print(decrypt(enc))
        except:
          print(response.data)
    else:
      print(response.data)

def shell(stub):
    while True:
      user_input = input("opaque> " )
      while user_input:
        send_query(stub, user_input)
        user_input = input("opaque> ")

def clean_up(channel):
    lib.sp_clean(sp)
    channel.close()
    print("Channel closed")

def run():
    channel = grpc.insecure_channel('localhost:50060')
    stub = rpc_pb2_grpc.OpaqueRPCStub(channel)

    atexit.register(clean_up, channel=channel)

    # Perform ra
    perform_ra(stub)

    # Start shell
    shell(stub)

if __name__ == '__main__':
    run()
