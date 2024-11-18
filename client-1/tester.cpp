#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "pfs_common/pfs_common.hpp"
#include "pfs_client/pfs_api.hpp"

// Helper function to print metadata
int pfs_print_meta(int pfs_fd, int client_id) {
    struct pfs_metadata mymeta = {0};
    int ret = pfs_fstat(pfs_fd, &mymeta);
    if (ret != -1) {
        printf("%s: PFS fd: %d, Client id: %d\n", __func__, pfs_fd, client_id);
        printf("%s: File name: %s, size: %lu\n", __func__, mymeta.filename, mymeta.file_size);
        printf("%s: Time of creation: %s", __func__, ctime(&(mymeta.ctime)));
        printf("%s: Last modification: %s", __func__, ctime(&(mymeta.mtime)));
    }
    return ret;
}

// Client 1 creates a file and writes to it
void client1_scenario() {
    printf("Client 1: Initializing PFS client\n");
    int client_id = pfs_initialize();
    if (client_id == -1) {
        fprintf(stderr, "Client 1: Failed to initialize PFS.\n");
        return;
    }

    // Create a file with 100 bytes
    if (pfs_create("pfs_file1", 1) == -1) {
        fprintf(stderr, "Client 1: Failed to create file.\n");
        return;
    }
    printf("Client 1: Created file 'pfs_file1'\n");

    // int pfs_fd = pfs_open("pfs_file1", 2);
    // if (pfs_fd == -1) {
    //     fprintf(stderr, "Client 1: Failed to open file.\n");
    //     return;
    // }

    char buffer[100];
    memset(buffer, 'A', 100); // Fill with dummy data
    if (pfs_write(1, buffer, 100, 0) == -1) {
        fprintf(stderr, "Client 1: Failed to write to file.\n");
        return;
    }
    printf("Client 1: Wrote 100 bytes to 'pfs_file1'\n");

    pfs_finish(1);

    // if (pfs_print_meta(pfs_fd, client_id) == -1) {
    //     fprintf(stderr, "Client 1: Failed to print metadata.\n");
    //     return;
    // }

    // if (pfs_close(pfs_fd) == -1) {
    //     fprintf(stderr, "Client 1: Failed to close file.\n");
    //     return;
    // }
    // if (pfs_finish(client_id) == -1) {
    //     fprintf(stderr, "Client 1: Failed to finish PFS.\n");
    //     return;
    // }
    printf("Client 1: Finished execution.\n");
}

// Client 2 requests a write token for a range
void client2_scenario() {
    printf("Client 2: Initializing PFS client\n");
    int client_id = pfs_initialize();
    if (client_id == -1) {
        fprintf(stderr, "Client 2: Failed to initialize PFS.\n");
        return;
    }

    // int pfs_fd = pfs_open("pfs_file1", 2);
    // if (pfs_fd == -1) {
    //     fprintf(stderr, "Client 2: Failed to open file.\n");
    //     return;
    // }

    char buffer[25];
    memset(buffer, 'B', 25); // Fill with dummy data
    if (pfs_write(1, buffer, 25, 25) == -1) {
        fprintf(stderr, "Client 2: Failed to write to file.\n");
        return;
    }
    printf("Client 2: Wrote 25 bytes to 'pfs_file1' at offset 25\n");

    // if (pfs_print_meta(pfs_fd, client_id) == -1) {
    //     fprintf(stderr, "Client 2: Failed to print metadata.\n");
    //     return;
    // }

    // if (pfs_close(pfs_fd) == -1) {
    //     fprintf(stderr, "Client 2: Failed to close file.\n");
    //     return;
    // }
    // if (pfs_finish(client_id) == -1) {
    //     fprintf(stderr, "Client 2: Failed to finish PFS.\n");
    //     return;
    // }
    
    pfs_finish(2);
    printf("Client 2: Finished execution.\n");
}

int main(int argc, char *argv[]) {
    if (argc < 2) {
        fprintf(stderr, "Usage: %s client1|client2\n", argv[0]);
        return -1;
    }

    if (strcmp(argv[1], "client1") == 0) {
        client1_scenario();
    } else if (strcmp(argv[1], "client2") == 0) {
        client2_scenario();
    } else {
        fprintf(stderr, "Invalid argument. Use 'client1' or 'client2'.\n");
        return -1;
    }
    sleep(200);

    return 0;
}
