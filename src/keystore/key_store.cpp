/*
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements. See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership. The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing,
  * software distributed under the License is distributed on an
  * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  * KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations
  * under the License.
  */

#include <string.h>
#include <unistd.h>
#include <arpa/inet.h> 

#include "truce_client.h"


void sendErrorMessage(int connfd) {
  int tmp_int = htonl(4);
  write(connfd, &tmp_int, 4);
  tmp_int = htonl(-1); //TODO message type
  write(connfd, &tmp_int, 4);
  close(connfd);
}


int main(int argc, char* argv[])
{
  const char* truce_server_address = argv[1];
  int my_port = -1;

  if (NULL == truce_server_address) {
    printf("Error: truce server address not specified in cmd line\n");
    printf("Syntax: truce_server_address[:port] [key_server_port]\n");
    return -1;
  }

  if (!truce_client_init(truce_server_address)) {
    printf("Error: failed to init truce client. Server address: %s\n", truce_server_address);
    return -1;
  }

  if (NULL != argv[2]) {
    my_port = std::stoi(argv[2]);
  }
  else {
    my_port = 48123;
  }

  // Get data encryption key
  // TODO Placeholder. Replace with real key management code.
  uint8_t data_key[16];
  FILE *key_file = fopen("key_file","rb");
  if (NULL == key_file) {
    for (int i=0; i < 16; i++) {data_key[i] = i;}
    key_file = fopen("key_file","wb");
    if (16 != fwrite(data_key, 1, 16, key_file)) {
      printf("ERROR: failed to write 16 bytes to key_file\n");
      return -1;
    }
    printf("Using a default data key\n");
  }
  else {
    if (16 != fread(data_key, 1, 16, key_file)) {
      printf("ERROR: failed to read 16 bytes from key_file\n");
      return -1;
    }
    printf("Using a data key loaded from key_file\n");
  }
  fclose(key_file);


  // Open server TCP port for incoming key requests
  int listenfd = 0, connfd = 0;

  if (!inet_listen(listenfd, my_port)) {
    printf("ERROR: Failed to listen on port %d.\n", my_port);
    return -1;
  }

  while(1)
  {
    // Listen to incoming connections
    printf("\nKeyStore: Waiting for incoming TCP connections on port %d\n", my_port);
    if (!inet_accept(connfd, listenfd)) {printf("ERROR: inet_accept has failed.\n");}

    // Receive length of Truce ID
    int tmp_int;
    if (!read_all(connfd, (uint8_t *) &tmp_int, 4)) {
      printf("Warning: failed to read first 4 bytes from connfd %d\n", connfd);
      sendErrorMessage(connfd);
      continue;
    }
    int enc_descr_len = ntohl(tmp_int);
    printf("Key request, enclave id lenth: %d \n", enc_descr_len);

    truce_id_t t_id = {{0}};

    // Receive Truce ID
    if (!read_all(connfd, (uint8_t *) &t_id, sizeof(t_id))) {
      printf("ERROR: failed to read %lu bytes of t_id\n", sizeof(t_id));
      sendErrorMessage(connfd);
      continue;
    }
    printf("Received enclave truce id\n");

    // Get the enclave record from TruCE server
    truce_record_t t_rec;

    if (!truce_client_recv_enclave_record(t_id, t_rec)) {
      printf("ERROR: failed to receive truce record from truce server\n");
      sendErrorMessage(connfd);
      continue;
    }

    sgx_quote_t quote = {0};

    // Verify enclave and signer measurements
    // TODO: At this point, the client should know what is the expected mrenclave and mrsigner.
    // for the simplicity of this code, we set the expected measurements to the given measurements.
    if (!truce_client_extract_quote_from_record(t_rec, quote)) {
      printf("ERROR: failed to extract quote from record\n");
      sendErrorMessage(connfd);
      continue;
    }

    // TODO: should be calculated from a given SO file.
    /*memcpy((void *) &expected_mrenclave, 
      (void *) &quote.report_body.mr_enclave, sizeof(sgx_measurement_t));
    memcpy((void *) &expected_mrsigner, (void *) &quote.report_body.mr_signer, sizeof(sgx_measurement_t));

    if (!truce_client_verify_enclave_record(
      t_id, t_rec, expected_mrenclave, expected_mrsigner)) {

        printf("ERROR: failed to verify enclave's record\n");
        sendErrorMessage(connfd);
        continue;
    }*/


    // Send a secret (here - a data key) to the enclave
    // TODO Example code:
    int data_key_id = 57; // You can send multiple data keys to the enclave

    // Your secret is the data key with its id (or any other meta-data you want to attach to your key)
    int secret_len = 4 + 16;
    uint8_t secret[secret_len];
    memcpy(secret, &data_key_id, 4);
    memcpy(secret+4, data_key, 16);

    uint8_t *encrypted_secret = NULL;
    uint32_t encrypted_secret_size = 0;

    // Encrypting secrets using Enclave's RSA public key.
    if (!truce_client_encrypt_secret(
          t_rec, secret, secret_len, encrypted_secret, encrypted_secret_size)) {
      printf("ERROR: failed to encrypt secret\n");
      sendErrorMessage(connfd);
      continue;
    }

    // Sending encrypted secret to application (and enclave)
    tmp_int = htonl(encrypted_secret_size + 8);
    write(connfd, &tmp_int, 4);
    tmp_int = htonl(0); //TODO message type
    write(connfd, &tmp_int, 4);
    tmp_int = htonl(encrypted_secret_size);
    write(connfd, &tmp_int, 4);
    write(connfd, encrypted_secret, encrypted_secret_size);

    printf("Sent encrypted secret to enclave \n");

    close(connfd);
  }
}
