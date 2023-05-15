#include <stdexcept>
#include <Access/SSH/SSHPublicKey.h>
#include <Common/SSH/clibssh.h>

namespace ssh
{

SSHPublicKey::SSHPublicKey(ssh_key key, bool own) : key_(key, own ? &deleter : &disabled_deleter)
{ // disable deleter if class is constructed without ownership
    if (!key_)
    {
        throw std::logic_error("No ssh_key provided in explicit constructor");
    }
}

SSHPublicKey::~SSHPublicKey() = default;

SSHPublicKey::SSHPublicKey(const SSHPublicKey & other) : key_(ssh_key_dup(other.get()), &deleter)
{
    if (!key_)
    {
        throw std::runtime_error("Failed to duplicate ssh_key");
    }
}

SSHPublicKey & SSHPublicKey::operator=(const SSHPublicKey & other)
{
    if (this != &other)
    {
        ssh_key newKey = ssh_key_dup(other.get());
        if (!newKey)
        {
            throw std::runtime_error("Failed to duplicate ssh_key");
        }
        key_.reset(newKey);
    }
    return *this;
}

SSHPublicKey::SSHPublicKey(SSHPublicKey && other) noexcept = default;

SSHPublicKey & SSHPublicKey::operator=(SSHPublicKey && other) noexcept = default;

bool SSHPublicKey::operator==(const SSHPublicKey & other) const
{
    return isEqual(other);
}

ssh_key SSHPublicKey::get() const
{
    return key_.get();
}

bool SSHPublicKey::isEqual(const SSHPublicKey & other) const
{
    int rc = ssh_key_cmp(key_.get(), other.get(), SSH_KEY_CMP_PUBLIC);
    return rc == 0;
}

SSHPublicKey SSHPublicKey::createFromBase64(const String & base64, const String & keyType)
{
    ssh_key key;
    int rc = ssh_pki_import_pubkey_base64(base64.c_str(), ssh_key_type_from_name(keyType.c_str()), &key);
    if (rc != SSH_OK)
    {
        throw std::invalid_argument("Bad ssh public key provided");
    }
    return SSHPublicKey(key);
}

SSHPublicKey SSHPublicKey::createFromFile(const std::string & filename)
{
    ssh_key key;
    int rc = ssh_pki_import_pubkey_file(filename.c_str(), &key);
    if (rc != SSH_OK)
    {
        if (rc == SSH_EOF)
        {
            throw std::invalid_argument("Can't import ssh public key from file as it doesn't exist or permission denied");
        }
        throw std::runtime_error("Can't import ssh public key from file");
    }
    return SSHPublicKey(key);
}

SSHPublicKey SSHPublicKey::createNonOwning(ssh_key key)
{
    return SSHPublicKey(key, false);
}

namespace
{

    struct CStringDeleter
    {
        [[maybe_unused]] void operator()(char * ptr) const { std::free(ptr); }
    };

}

String SSHPublicKey::getBase64Representation() const
{
    char * buf = nullptr;
    int rc = ssh_pki_export_pubkey_base64(key_.get(), &buf);

    if (rc != SSH_OK)
    {
        throw std::runtime_error("Failed to export public key to base64");
    }

    // Create a String from cstring, which makes a copy of the first one and requires freeing memory after it
    std::unique_ptr<char, CStringDeleter> buf_ptr(buf); // This is to safely manage buf memory
    return String(buf_ptr.get());
}

void SSHPublicKey::deleter(ssh_key key)
{
    ssh_key_free(key);
}

}
