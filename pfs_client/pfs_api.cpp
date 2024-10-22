#include "pfs_api.hpp"
#include "pfs_cache.hpp"
#include "pfs_metaserver/pfs_metaserver_api.hpp"
#include "pfs_fileserver/pfs_fileserver_api.hpp"

int pfs_initialize() {
    // Read pfs_list.txt

    // Check if all servers (NUM_FILE_SERVERS + 1) are online
    // Connect with metaserver using gRPC
    metaserver_api_temp();

    // Connect with all fileservers (NUM_FILE_SERVERS) using gRPC
    for (int i = 0; i < NUM_FILE_SERVERS; ++i) {
        fileserver_api_temp();
    }

    int client_id = 1;

    return client_id;
}

int pfs_finish(int client_id) {

    return 0;
}

int pfs_create(const char *filename, int stripe_width) {

    return 0;
}

int pfs_open(const char *filename, int mode) {

    return 0;
}

int pfs_read(int fd, void *buf, size_t num_bytes, off_t offset) {
    // ...

    // Check client cache
    cache_func_temp();

    // ...

    return 0;
}

int pfs_write(int fd, const void *buf, size_t num_bytes, off_t offset) {
    // ...

    // Check client cache
    cache_func_temp();

    // ...

    return 0;
}

int pfs_close(int fd) {

    return 0;
}

int pfs_delete(const char *filename) {

    return 0;
}

int pfs_fstat(int fd, struct pfs_metadata *meta_data) {

    return 0;
}

int pfs_execstat(struct pfs_execstat *execstat_data) {

    return 0;
}
