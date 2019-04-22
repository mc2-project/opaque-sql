#include "ServiceProvider.h"

int main(int argc, char **argv) {
  if (argc < 2) {
    printf("Usage: ./keygen <public_key_cpp_file>\n");
    return 1;
  }

  std::string private_key_filename(std::getenv("PRIVATE_KEY_PATH"));
  service_provider.load_private_key(private_key_filename);
  service_provider.export_public_key_code(argv[1]);

  return 0;
}
