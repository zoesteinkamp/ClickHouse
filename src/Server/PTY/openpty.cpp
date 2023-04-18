#include <Server/PTY/openpty.h>
#include <cstdlib>
#include <fcntl.h>
#include <termios.h>
#include <unistd.h>
#include <sys/ioctl.h>

int openpty(int *amaster, int *aslave, char *name,
            const struct termios *termp, const struct winsize *winp)
{
    int master, slave;
    char slave_name[256];

    master = posix_openpt(O_RDWR | O_NOCTTY);
    if (master < 0) {
        return -1;
    }

    if (grantpt(master) < 0 || unlockpt(master) < 0) {
        close(master);
        return -1;
    }

    if (ptsname_r(master, slave_name, sizeof(slave_name)) != 0) {
        close(master);
        return -1;
    }

    if (name) {
        strncpy(name, slave_name, strlen(slave_name) + 1);
    }

    slave = open(slave_name, O_RDWR | O_NOCTTY);
    if (slave < 0) {
        close(master);
        return -1;
    }

    if (amaster) {
        *amaster = master;
    }
    if (aslave) {
        *aslave = slave;
    }

    if (termp) {
        tcsetattr(slave, TCSANOW, termp);
    }
    if (winp) {
        ioctl(slave, TIOCSWINSZ, winp);
    }

    return 0;
}
