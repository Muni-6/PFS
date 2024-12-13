#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <thread>
#include <vector>
#include <random>
#include <chrono>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <cassert>
#include <algorithm>
#include <memory>

#include "pfs_common/pfs_common.hpp"
#include "pfs_client/pfs_api.hpp"

/*
  Even More Complex Token Range Test Under 100MB:

  Configuration:
  - 8 files × 8MB = 64MB total.
  - 64 worker threads × 1000 ops each.
  - 4 verifier threads.
  - Operations come in various "modes":
    1) Small operations: very small reads/writes near stable region boundaries.
    2) Medium operations: moderate length reads/writes at random offsets.
    3) Large operations: large sections of the file (up to a few MB).
    4) Full-file or large part reads occasionally, forcing large token requests.

  This randomness in offset and length distribution will cause the metadata server
  to handle many different token ranges—small, large, overlapping, disjoint—thus testing
  token management thoroughly.
*/

static const char *LOCAL_INPUT_FILE = "/home/grads/nkv5154/Project2/p2-pfs-mzk6126-nkv5154/client-1/input.txt";
static const size_t NUM_FILES = 8;
static const size_t TEST_FILE_SIZE = 8 * 1024 * 1024; // 8MB
static const size_t NUM_WORKER_THREADS = 64;
static const size_t OPS_PER_THREAD = 1000;
static const size_t MAX_RW_SIZE = 8192;        // 8KB max for small ops
static const size_t STABLE_REGION_SIZE = 64 * 1024; // 64KB stable region
static const int STRIPE_WIDTH = 4;
static const int NUM_VERIFIER_THREADS = 4;

static std::atomic<bool> start_flag(false);
static std::atomic<bool> stop_verification(false);



extern int pfs_execstat(struct pfs_execstat *stat);

int pfs_print_stat(int client_id) {
    struct pfs_execstat mystat = {0};
    int ret = pfs_execstat(&mystat);
    if (ret != -1) {
        printf("pfs_print_stat: client_id: %d\n", client_id);
        printf("num_read_hits: %ld, num_write_hits: %ld\n", mystat.num_read_hits, mystat.num_write_hits);
        printf("num_evictions: %ld, num_writebacks: %ld, num_invalidations: %ld\n",
               mystat.num_evictions, mystat.num_writebacks, mystat.num_invalidations);
        printf("num_close_writebacks: %ld, num_close_evictions: %ld\n",
               mystat.num_close_writebacks, mystat.num_close_evictions);
    }
    return ret;
}

struct TestFileInfo {
    std::string pfs_name;
    size_t offset_in_local;
    bool exists;
    std::mutex mtx;

    TestFileInfo(const std::string &name, size_t offset, bool file_exists)
        : pfs_name(name), offset_in_local(offset), exists(file_exists) {}

    TestFileInfo(const TestFileInfo&) = delete;
    TestFileInfo& operator=(const TestFileInfo&) = delete;
    TestFileInfo(TestFileInfo&&) = delete;
    TestFileInfo& operator=(TestFileInfo&&) = delete;
};

static std::vector<std::unique_ptr<TestFileInfo>> test_files;

static void verify_stable_region_full(const TestFileInfo &tfile, const char *local_buf) {
    int fd = pfs_open(tfile.pfs_name.c_str(), 1);
    if (fd == -1) return;
    std::vector<char> buf(STABLE_REGION_SIZE, 0);
    int ret = pfs_read(fd, buf.data(), (int)STABLE_REGION_SIZE, 0);
    pfs_close(fd);
    if (ret == (int)STABLE_REGION_SIZE) {
        const char *expected = local_buf + tfile.offset_in_local;
        if (memcmp(buf.data(), expected, STABLE_REGION_SIZE) != 0) {
            fprintf(stderr, "[VERIFIER] Full stable region mismatch in file %s.\n", tfile.pfs_name.c_str());
        }
    }
}

static void verify_stable_region_partial(const TestFileInfo &tfile, const char *local_buf) {
    int fd = pfs_open(tfile.pfs_name.c_str(), 1);
    if (fd == -1) return;
    std::default_random_engine eng(std::random_device{}());
    std::uniform_int_distribution<size_t> off_dist(0, STABLE_REGION_SIZE-1);
    std::uniform_int_distribution<size_t> len_dist(1, 1024);

    size_t start = off_dist(eng);
    size_t length = len_dist(eng);
    if (start + length > STABLE_REGION_SIZE) {
        length = STABLE_REGION_SIZE - start;
    }

    std::vector<char> buf(length, 0);
    int ret = pfs_read(fd, buf.data(), (int)length, (int)start);
    pfs_close(fd);

    if (ret == (int)length) {
        const char *expected = local_buf + tfile.offset_in_local + start;
        if (memcmp(buf.data(), expected, length) != 0) {
            fprintf(stderr, "[VERIFIER] Partial stable region mismatch (offset=%zu, len=%zu) in file %s.\n",
                    start, length, tfile.pfs_name.c_str());
        }
    }
}

void verifier_thread_func(const char *local_buf, size_t local_buf_size) {
    std::default_random_engine eng(std::random_device{}());
    std::uniform_int_distribution<int> file_dist(0, (int)NUM_FILES-1);
    std::uniform_int_distribution<int> sleep_dist(50, 1000);
    std::uniform_int_distribution<int> verify_mode_dist(0, 3);

    while (!start_flag.load()) {
        std::this_thread::yield();
    }

    while (!stop_verification.load()) {
        int fidx = file_dist(eng);
        auto &tfile = test_files[fidx];
        {
            std::lock_guard<std::mutex> lk(tfile->mtx);
            if (!tfile->exists) {
                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_dist(eng)));
                continue;
            }
        }

        int mode = verify_mode_dist(eng);
        if (mode == 0) {
            verify_stable_region_full(*tfile, local_buf);
        } else {
            verify_stable_region_partial(*tfile, local_buf);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(sleep_dist(eng)));
    }
}

void worker_thread_func(int thread_id, const char *local_buf, size_t local_buf_size, int client_id) {
    while (!start_flag.load()) {
        std::this_thread::yield();
    }

    std::default_random_engine eng(std::random_device{}());
    std::uniform_int_distribution<int> file_dist(0, (int)NUM_FILES-1);
    std::uniform_int_distribution<int> op_dist(0, 19);
    std::uniform_int_distribution<int> pause_dist(1, 50);

    // Additional distributions for different mode selections
    std::uniform_int_distribution<int> mode_dist(0, 9); 
    // Modes:
    // 0-3: small ops (tiny segments, often near stable region)
    // 4-5: medium ops (moderate length)
    // 6-7: large ops (hundreds of KB to MB)
    // 8-9: very large ops (up to a big chunk of the file)
    
    for (size_t i = 0; i < OPS_PER_THREAD; i++) {
        int fidx = file_dist(eng);
        auto &tfile = test_files[fidx];

        int op = op_dist(eng);

        // Determine operation size and offsets based on mode
        int mode = mode_dist(eng);
        size_t offset = 0, length = 0;

        // Choose offsets and lengths depending on mode
        // We'll create random logic to produce various range sizes
        if (mode <= 3) {
            // Small ops: length up to MAX_RW_SIZE, often near stable region
            std::uniform_int_distribution<size_t> near_stable(0, STABLE_REGION_SIZE*2);
            offset = near_stable(eng);
            if (offset > TEST_FILE_SIZE - 1) offset = TEST_FILE_SIZE - 1;
            length = (size_t)(1 + (eng() % MAX_RW_SIZE));
            if (offset + length > TEST_FILE_SIZE) {
                length = TEST_FILE_SIZE - offset;
            }
        } else if (mode <= 5) {
            // Medium ops: a few KB to tens of KB
            std::uniform_int_distribution<size_t> off_dist(0, TEST_FILE_SIZE - 1);
            offset = off_dist(eng);
            // length up to maybe 64KB
            std::uniform_int_distribution<size_t> len_dist(1, 64*1024);
            length = len_dist(eng);
            if (offset + length > TEST_FILE_SIZE) {
                length = TEST_FILE_SIZE - offset;
            }
        } else if (mode <= 7) {
            // Large ops: up to 1MB
            std::uniform_int_distribution<size_t> off_dist(0, TEST_FILE_SIZE - 1);
            offset = off_dist(eng);
            std::uniform_int_distribution<size_t> len_dist(1, 1024*1024);
            length = len_dist(eng);
            if (offset + length > TEST_FILE_SIZE) {
                length = TEST_FILE_SIZE - offset;
            }
        } else {
            // Very large ops: up to ~1/2 the file
            std::uniform_int_distribution<size_t> off_dist(0, TEST_FILE_SIZE - 1);
            offset = off_dist(eng);
            std::uniform_int_distribution<size_t> len_dist(1, TEST_FILE_SIZE/2);
            length = len_dist(eng);
            if (offset + length > TEST_FILE_SIZE) {
                length = TEST_FILE_SIZE - offset;
            }
        }

        // Avoid zero-length
        if (length == 0) length = 1;

        if (op == 0) {
            // Delete & recreate
            {
                std::lock_guard<std::mutex> lk(tfile->mtx);
                if (tfile->exists) {
                    if (pfs_delete(tfile->pfs_name.c_str()) == 0) {
                        tfile->exists = false;
                    }
                }
            }
            if (pfs_create(tfile->pfs_name.c_str(), STRIPE_WIDTH) == 0) {
                int fd = pfs_open(tfile->pfs_name.c_str(), 2);
                if (fd != -1) {
                    size_t stable_sz = std::min(STABLE_REGION_SIZE, TEST_FILE_SIZE);
                    const char *src = local_buf + tfile->offset_in_local;
                    int ret = pfs_write(fd, (void*)src, (int)stable_sz, 0);
                    if (ret == (int)stable_sz) {
                        std::lock_guard<std::mutex> lk(tfile->mtx);
                        tfile->exists = true;
                    }
                    pfs_close(fd);
                }
            }
        } else if (op < 2) {
            // Open/close read-only
            std::lock_guard<std::mutex> lk(tfile->mtx);
            if (!tfile->exists) {
                std::this_thread::sleep_for(std::chrono::milliseconds(pause_dist(eng)));
                continue;
            }
            int fd = pfs_open(tfile->pfs_name.c_str(), 1);
            if (fd != -1) pfs_close(fd);
        } else if (op < 4) {
            // Open/close read-write
            std::lock_guard<std::mutex> lk(tfile->mtx);
            if (!tfile->exists) {
                std::this_thread::sleep_for(std::chrono::milliseconds(pause_dist(eng)));
                continue;
            }
            int fd = pfs_open(tfile->pfs_name.c_str(), 2);
            if (fd != -1) pfs_close(fd);
        } else if (op < 10) {
            // Writes
            std::lock_guard<std::mutex> lk(tfile->mtx);
            if (!tfile->exists) {
                std::this_thread::sleep_for(std::chrono::milliseconds(pause_dist(eng)));
                continue;
            }
            int fd = pfs_open(tfile->pfs_name.c_str(), 2);
            if (fd == -1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(pause_dist(eng)));
                continue;
            }
            const char *src = local_buf + tfile->offset_in_local + offset;
            int ret = pfs_write(fd, src, (int)length, (int)offset);
            if (ret == -1) {
                fprintf(stderr, "Thread %d: pfs_write error on file %s offset %zu length %zu.\n",
                        thread_id, tfile->pfs_name.c_str(), offset, length);
            }
            pfs_close(fd);
        } else {
            // Reads
            std::lock_guard<std::mutex> lk(tfile->mtx);
            if (!tfile->exists) {
                std::this_thread::sleep_for(std::chrono::milliseconds(pause_dist(eng)));
                continue;
            }
            int fd = pfs_open(tfile->pfs_name.c_str(), 1);
            if (fd == -1) {
                std::this_thread::sleep_for(std::chrono::milliseconds(pause_dist(eng)));
                continue;
            }

            std::vector<char> read_buf(length, 0);
            int ret = pfs_read(fd, read_buf.data(), (int)length, (int)offset);
            // If ret < length and ret != -1, it's just EOF scenario, so no error unless offset is invalid
            pfs_close(fd);

            // Optional: we could verify stable region if fully inside stable region
            if (ret == (int)length && offset < STABLE_REGION_SIZE && offset + length <= STABLE_REGION_SIZE) {
                const char *expected = local_buf + tfile->offset_in_local + offset;
                if (memcmp(read_buf.data(), expected, length) != 0) {
                    fprintf(stderr, "Thread %d: Stable region verification failed in read for %s.\n",
                            thread_id, tfile->pfs_name.c_str());
                }
            }
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(pause_dist(eng)));
    }
}

int main(int argc, char *argv[]) {
    int input_fd = open(LOCAL_INPUT_FILE, O_RDONLY);
    if (input_fd == -1) {
        fprintf(stderr, "Failed to open local input file %s: %s\n", LOCAL_INPUT_FILE, strerror(errno));
        return -1;
    }

    off_t need_size = (off_t)NUM_FILES * TEST_FILE_SIZE;
    off_t fsize = lseek(input_fd, 0, SEEK_END);
    if (fsize < need_size) {
        fprintf(stderr, "Local file %s is too small (%ld bytes) for test size %ld\n",
                LOCAL_INPUT_FILE, (long)fsize, (long)need_size);
        close(input_fd);
        return -1;
    }
    lseek(input_fd, 0, SEEK_SET);

    char *local_buf = (char *)malloc(need_size);
    if (!local_buf) {
        fprintf(stderr, "Memory allocation failed.\n");
        close(input_fd);
        return -1;
    }

    ssize_t nread = read(input_fd, local_buf, need_size);
    if (nread != (ssize_t)need_size) {
        fprintf(stderr, "Failed to read enough data.\n");
        free(local_buf);
        close(input_fd);
        return -1;
    }
    close(input_fd);

    int client_id = pfs_initialize();
    if (client_id == -1) {
        fprintf(stderr, "pfs_initialize() failed.\n");
        free(local_buf);
        return -1;
    }

    test_files.reserve(NUM_FILES);
    for (size_t i = 0; i < NUM_FILES; i++) {
        std::string pfs_fname = "pfs_test_file" + std::to_string(i);
        size_t offset = i * TEST_FILE_SIZE;
        bool file_exists = false;
        if (pfs_create(pfs_fname.c_str(), STRIPE_WIDTH) == 0) {
            int fd = pfs_open(pfs_fname.c_str(), 2);
            if (fd != -1) {
                size_t stable_sz = std::min(STABLE_REGION_SIZE, TEST_FILE_SIZE);
                const char *src = local_buf + offset;
                int ret = pfs_write(fd, (void*)src, (int)stable_sz, 0);
                if (ret == (int)stable_sz) {
                    file_exists = true;
                }
                pfs_close(fd);
            }
        }
        test_files.emplace_back(std::make_unique<TestFileInfo>(pfs_fname, offset, file_exists));
    }

    std::vector<std::thread> worker_threads;
    worker_threads.reserve(NUM_WORKER_THREADS);
    for (size_t i = 0; i < NUM_WORKER_THREADS; i++) {
        worker_threads.emplace_back(worker_thread_func, (int)i, local_buf, (size_t)need_size, client_id);
    }

    std::vector<std::thread> verifier_threads;
    verifier_threads.reserve(NUM_VERIFIER_THREADS);
    for (int i = 0; i < NUM_VERIFIER_THREADS; i++) {
        verifier_threads.emplace_back(verifier_thread_func, local_buf, (size_t)need_size);
    }

    start_flag.store(true);

    for (auto &th : worker_threads) {
        th.join();
    }

    stop_verification.store(true);
    for (auto &vth : verifier_threads) {
        vth.join();
    }

    // Final verification
    bool all_correct = true;
    for (size_t i = 0; i < NUM_FILES; i++) {
        auto &tfile = test_files[i];
        std::lock_guard<std::mutex> lk(tfile->mtx);
        if (!tfile->exists) {
            printf("File %s does not exist at end of test.\n", tfile->pfs_name.c_str());
            continue;
        }
        int fd = pfs_open(tfile->pfs_name.c_str(), 1);
        if (fd == -1) {
            printf("File %s cannot be opened at end of test.\n", tfile->pfs_name.c_str());
            all_correct = false;
            continue;
        }

        std::vector<char> buf(STABLE_REGION_SIZE, 0);
        int ret = pfs_read(fd, buf.data(), (int)STABLE_REGION_SIZE, 0);
        pfs_close(fd);

        if (ret == (int)STABLE_REGION_SIZE) {
            const char *expected = local_buf + tfile->offset_in_local;
            if (memcmp(buf.data(), expected, STABLE_REGION_SIZE) != 0) {
                printf("Stable region mismatch at end for file %s.\n", tfile->pfs_name.c_str());
                all_correct = false;
            } else {
                printf("Stable region verified for file %s.\n", tfile->pfs_name.c_str());
            }
        } else {
            printf("Could not read stable region from file %s at the end.\n", tfile->pfs_name.c_str());
            all_correct = false;
        }
    }

    pfs_print_stat(client_id);
    int finish_ret = pfs_finish(client_id);
    if (finish_ret == -1) {
        fprintf(stderr, "pfs_finish() failed.\n");
    }

    free(local_buf);
    printf("Complex token range stress test completed.\n");
    if (all_correct) {
        printf("All stable regions verified successfully. Test passed.\n");
    } else {
        printf("Some stable regions failed verification. Check logs.\n");
    }

    return all_correct ? 0 : 1;
}