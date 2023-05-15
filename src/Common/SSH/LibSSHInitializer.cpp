#include "LibSSHInitializer.h"

namespace ssh
{

LibSSHInitializer::LibSSHInitializer()
{
    int rc = ssh_init();
    if (rc != SSH_OK)
    {
        throw std::runtime_error("Failed to initialize libssh");
    }
}

LibSSHInitializer::~LibSSHInitializer()
{
    ssh_finalize();
}

}
