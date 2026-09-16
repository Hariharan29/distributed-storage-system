// =============================================================================
// sha256.cpp — SHA-256 implementation using OpenSSL's EVP API
//
// OpenSSL is available in our Docker image (libssl-dev).
// The EVP_* API is the modern, recommended way to use OpenSSL hashing in C++.
//
// Python equivalent of this file:
//   import hashlib
//   def hash(data: bytes) -> str:
//       return hashlib.sha256(data).hexdigest()
// =============================================================================

#include "sha256.h"

#include <openssl/evp.h>   // EVP_MD_CTX, EVP_sha256, etc.
#include <fstream>         // std::ifstream for reading files
#include <iomanip>         // std::setw, std::setfill for hex formatting
#include <sstream>         // std::ostringstream for building the hex string
#include <stdexcept>       // std::runtime_error

// ── Internal helper ──────────────────────────────────────────────────────────
// Converts a raw 32-byte digest into a 64-character lowercase hex string.
// This is the C++ equivalent of `.hexdigest()` in Python's hashlib.
static std::string digestToHex(const unsigned char* digest, unsigned int len) {
    std::ostringstream oss;
    for (unsigned int i = 0; i < len; ++i) {
        // setw(2)      → always print 2 characters wide
        // setfill('0') → pad with '0' if the byte is < 0x10
        // hex          → print in base-16
        oss << std::hex << std::setw(2) << std::setfill('0')
            << static_cast<int>(digest[i]);
    }
    return oss.str();
}

// ── Core hashing function using EVP ──────────────────────────────────────────
// EVP (Envelope) is OpenSSL's high-level API. Think of it as a context object
// that you feed data into, then finalise to get the digest.
static std::string computeHash(EVP_MD_CTX* ctx, const unsigned char* data, size_t len) {
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int  digest_len = 0;

    if (EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
        throw std::runtime_error("SHA256: EVP_DigestInit_ex failed");
    }
    if (EVP_DigestUpdate(ctx, data, len) != 1) {
        throw std::runtime_error("SHA256: EVP_DigestUpdate failed");
    }
    if (EVP_DigestFinal_ex(ctx, digest, &digest_len) != 1) {
        throw std::runtime_error("SHA256: EVP_DigestFinal_ex failed");
    }

    return digestToHex(digest, digest_len);
}

// ─────────────────────────────────────────────────────────────────────────────

std::string SHA256Util::hash(const std::vector<uint8_t>& data) {
    // EVP_MD_CTX is like a "hasher object" in Python (hashlib.sha256())
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) throw std::runtime_error("SHA256: Failed to create EVP_MD_CTX");

    std::string result;
    try {
        result = computeHash(ctx,
                             reinterpret_cast<const unsigned char*>(data.data()),
                             data.size());
    } catch (...) {
        EVP_MD_CTX_free(ctx);
        throw;
    }

    EVP_MD_CTX_free(ctx);  // Always free; C++ doesn't have a garbage collector
    return result;
}

std::string SHA256Util::hash(const std::string& data) {
    // Reuse the vector overload by converting the string to bytes
    return hash(std::vector<uint8_t>(data.begin(), data.end()));
}

std::string SHA256Util::hashFile(const std::string& filepath) {
    // Open file in binary mode (important: without this, Windows may mangle bytes)
    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("SHA256: Cannot open file: " + filepath);
    }

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) throw std::runtime_error("SHA256: Failed to create EVP_MD_CTX");

    if (EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
        EVP_MD_CTX_free(ctx);
        throw std::runtime_error("SHA256: EVP_DigestInit_ex failed");
    }

    // Read and hash the file in 64KB blocks to avoid loading it all into memory.
    // This is similar to Python's:
    //   while chunk := f.read(65536): hasher.update(chunk)
    constexpr size_t READ_BLOCK = 65536;
    char buffer[READ_BLOCK];

    while (file.read(buffer, READ_BLOCK) || file.gcount() > 0) {
        if (EVP_DigestUpdate(ctx,
                             reinterpret_cast<const unsigned char*>(buffer),
                             static_cast<size_t>(file.gcount())) != 1) {
            EVP_MD_CTX_free(ctx);
            throw std::runtime_error("SHA256: EVP_DigestUpdate failed");
        }
    }

    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int  digest_len = 0;

    if (EVP_DigestFinal_ex(ctx, digest, &digest_len) != 1) {
        EVP_MD_CTX_free(ctx);
        throw std::runtime_error("SHA256: EVP_DigestFinal_ex failed");
    }

    EVP_MD_CTX_free(ctx);
    return digestToHex(digest, digest_len);
}
