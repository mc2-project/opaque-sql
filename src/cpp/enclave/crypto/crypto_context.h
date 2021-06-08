#include "crypto.h"

/*
 * This class generates a singleton containing a Crypto instance
 */
class CryptoContext {
private:
  CryptoContext() { crypto = new Crypto(); }

public:
  Crypto *crypto;

  // Don't forget to declare these two. You want to make sure they
  // are unacceptable otherwise you may accidentally get copies of
  // your singleton appearing.
  CryptoContext(CryptoContext const &) = delete;
  void operator=(CryptoContext const &) = delete;

  static CryptoContext &getInstance() {
    static CryptoContext instance;
    return instance;
  }
};
