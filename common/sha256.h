#pragma once

// =============================================================================
// sha256.h — SHA-256 hashing utility
//
// Python equivalent:
//   import hashlib
//   hashlib.sha256(data).hexdigest()
//
// This class wraps OpenSSL's SHA-256 implementation and returns a lowercase
// hex string, e.g.: "a3f5b2c1..."
//
// All methods are `static`, meaning you call them without creating an object:
//   std::string hash = SHA256Util::hash(myData);
// =============================================================================

#include <string>
#include <vector>
#include <cstdint>

class SHA256Util {
public:
    // Hash a block of raw bytes.
    // Input:  vector of bytes (e.g. the contents of a file chunk)
    // Output: 64-character lowercase hex string of the SHA-256 digest
    //
    // Example:
    //   std::vector<uint8_t> bytes = {'h', 'e', 'l', 'l', 'o'};
    //   std::string h = SHA256Util::hash(bytes);
    //   // h == "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    static std::string hash(const std::vector<uint8_t>& data);

    // Hash an entire file from disk.
    // Opens the file at `filepath`, reads it in chunks, and returns the SHA-256.
    // Useful for verifying file integrity without loading everything into memory.
    static std::string hashFile(const std::string& filepath);

    // Hash a raw string (convenience overload).
    static std::string hash(const std::string& data);
};
