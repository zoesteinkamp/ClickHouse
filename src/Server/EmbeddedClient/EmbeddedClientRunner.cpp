#include <Server/EmbeddedClient/EmbeddedClientRunner.h>
#include <Common/Exception.h>
#include "Common/logger_useful.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void EmbeddedClientRunner::run(const NameToNameMap & envs, const String & starting_query)
{
    if (started.test_and_set())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Client has been already started");
    }
    client_thread = ThreadFromGlobalPool(&EmbeddedClientRunner::clientRoutine, this, envs, starting_query);
}


void EmbeddedClientRunner::changeWindowSize(int width, int height, int width_pixels, int height_pixels)
{
    auto * ptyDescriptors = dynamic_cast<PtyClientDescriptorSet *>(client_descriptors.get());
    if (ptyDescriptors == nullptr)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Accessing window change on non pty descriptors");
    }
    ptyDescriptors->changeWindowSize(width, height, width_pixels, height_pixels);
}

EmbeddedClientRunner::~EmbeddedClientRunner()
{
    client_descriptors->closeServerDescriptors();
    if (client_thread.joinable())
    {
        client_thread.join();
    }
}

void EmbeddedClientRunner::clientRoutine(NameToNameMap envs, String starting_query)
{
    try
    {
        auto descr = client_descriptors->getDescriptorsForClient();
        auto stre = client_descriptors->getStreamsForClient();
        LocalServerPty client(std::move(dbSession), descr.in, descr.out, descr.err, stre.in, stre.out, stre.err);
        client.run(envs, starting_query);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
    finished.test_and_set();
    char c = 0;
    // Server may poll on a descriptor waiting for client output, wake him up with invisible character
    write(client_descriptors->getDescriptorsForClient().out, &c, 1);
}

}
