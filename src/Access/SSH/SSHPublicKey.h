#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <base/types.h>

struct ssh_key_struct;
using ssh_key = ssh_key_struct *;

namespace ssh
{


class SSHPublicKey
{
public:
    SSHPublicKey() = delete;
    ~SSHPublicKey();

    SSHPublicKey(const SSHPublicKey &);
    SSHPublicKey & operator=(const SSHPublicKey &);

    SSHPublicKey(SSHPublicKey &&) noexcept;
    SSHPublicKey & operator=(SSHPublicKey &&) noexcept;

    bool operator==(const SSHPublicKey &) const;

    ssh_key get() const;

    bool isEqual(const SSHPublicKey & other) const;

    String getBase64Representation() const;

    static SSHPublicKey createFromBase64(const String & base64, const String & key_type);

    static SSHPublicKey createFromFile(const String & filename);

    // Creates SSHPublicKey, but without owning the memory of ssh_key.
    // A user must manage it by himself. (This is implemented for compatibility with libssh callbacks)
    static SSHPublicKey createNonOwning(ssh_key key);

private:
    explicit SSHPublicKey(ssh_key key, bool own = true);

    static void deleter(ssh_key key);

    // We may want to not own ssh_key memory, so then we pass this deleter to unique_ptr
    static void disabledDeleter(ssh_key) { }

    std::unique_ptr<ssh_key_struct, decltype(&deleter)> key;
};

}
