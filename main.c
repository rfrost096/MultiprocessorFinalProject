#include "chained.h"
#include <omp.h>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <unistd.h>
#include <getopt.h>
#include <string.h>

#define MAX_LINE_LENGTH 256
#define INIT_NUM_BUCKETS 64
#define INIT_NUM_LOCKS_RATIO 8
#define MAX_TASK_POOL 256
#define FILE_CHUNK_SIZE 32768 

int end_of_file = 0;

typedef struct {
    FILE *file;
    char hash_op;
    uint64_t key;
    uint64_t value;
} FileIterator;

typedef struct {
    char hash_op;
    uint64_t key;
    uint64_t value;
} BatchItem;

typedef struct {
    uint64_t total_ops;
    uint64_t total_lookups;
    uint64_t successful_lookups;
    uint64_t missed_lookups;
    uint64_t total_inserts;
    uint64_t failed_match;
    double start;
    double end;

} MetricObject;

static inline const char* parse_line(const char* cursor, char* op, uint64_t* key, uint64_t* val) {
    while (*cursor == ' ' || *cursor == '\t' || *cursor == '\n') cursor++;
    if (*cursor == '\0') return NULL;
    *op = *cursor;
    cursor++;
    char* end_ptr;
    *key = strtoull(cursor, &end_ptr, 10);
    cursor = end_ptr;
    *val = strtoull(cursor, &end_ptr, 10);
    cursor = end_ptr;
    return cursor;
}

int main(int argc, char *argv[]) {

    int initial_buckets = INIT_NUM_BUCKETS;
    int num_threads = DEFAULT_NUM_THREADS;
    char* data_file = "output.txt";

    int opt;
    while ((opt = getopt(argc, argv, "f:b:t:rs")) != -1) {
        switch (opt) {
            case 'f':
                data_file = optarg;
                break;
            case 'b':
                initial_buckets = atoi(optarg);
                if (initial_buckets == 0) {
                    printf("start buckets must be > 0, setting to default\n");
                    initial_buckets = INIT_NUM_BUCKETS;
                }
                break;
            case 't':
                num_threads = atoi(optarg);
                if (num_threads < 1) {
                    printf("number of threads must be > 1, setting to default\n");
                    num_threads = DEFAULT_NUM_THREADS;
                }
                break;
            case 'r':
                resize_enabled = 0;
                break;
            case 's':
                speed_test = 1;
                break;
            default:
                printf("format to use: %s [-b initial_buckets] [-t num_threads] [-r disable_resize] [-s speed_test]\n", argv[0]);
                exit(1);
        }
    }

    omp_set_num_threads(num_threads);

    FILE *f = fopen(data_file, "rb");

    if (f == NULL) {
        printf("File not found\n");
        exit(1);
    }

    FileIterator iter = { .file = f };

    MetricObject run_metrics = {0};

    ChainedHashTable* chained = create_table(initial_buckets, initial_buckets / INIT_NUM_LOCKS_RATIO);

    run_metrics.start = omp_get_wtime();

    #pragma omp parallel
    {
        while (!end_of_file) {

            #pragma omp single
            {
                int temp_resize_needed = 0;
                int count = 0;
                while (1) {

                    char* buffer = malloc(FILE_CHUNK_SIZE + 1);
                    if (!buffer) exit(1);

                    size_t bytes_read = fread(buffer, 1, FILE_CHUNK_SIZE, f);

                    if (bytes_read == 0) {
                        free(buffer);
                        end_of_file = 1;
                        break;
                    }
                    if (bytes_read == FILE_CHUNK_SIZE) {

                        // Find the last newline for a clean break
                        char* last_newline = NULL;
                        for (size_t i = bytes_read; i > 0; i--) {
                            if (buffer[i-1] == '\n') {
                                last_newline = &buffer[i-1];
                                break;
                            }
                        }

                        if (last_newline) {
                            size_t valid_bytes = (last_newline - buffer) + 1;
                            fseek(f, -(long)(bytes_read - valid_bytes), SEEK_CUR);
                            bytes_read = valid_bytes;
                        }
                    }
                    
                    buffer[bytes_read] = '\0';

                    #pragma omp task firstprivate(buffer, bytes_read) shared(run_metrics, chained)
                    {
                        uint64_t temp_ops = 0;
                        uint64_t temp_lookups = 0;
                        uint64_t temp_succ_lookups = 0;
                        uint64_t temp_missed_lookups = 0;
                        uint64_t temp_inserts = 0;
                        uint64_t temp_failed_match = 0;

                        const char* cursor = buffer;
                        const char* end = buffer + bytes_read;
                        
                        char hash_op;
                        uint64_t key;
                        uint64_t value;

                        while (cursor < end && *cursor != '\0') {
                            cursor = parse_line(cursor, &hash_op, &key, &value);
                            if (!cursor) break;

                            temp_ops++;

                            if (hash_op == 'L') {
                                temp_lookups++;
                                uint64_t lookup_val = lookup(chained, key);

                                if (!speed_test) {
                                    if (lookup_val == INVALID_VALUE) {
                                        temp_missed_lookups++;
                                    } else {
                                        temp_succ_lookups++;
                                        if (lookup_val != value) {
                                            temp_failed_match++;
                                        }
                                    }
                                }
                            } else if (hash_op == 'I') {
                                temp_inserts++;
                                insert(chained, key, value);
                            }
                        }

                        free(buffer);

                        if (!speed_test) {
                            #pragma omp atomic
                            run_metrics.total_ops += temp_ops;
                            #pragma omp atomic
                            run_metrics.total_lookups += temp_lookups;
                            #pragma omp atomic
                            run_metrics.successful_lookups += temp_succ_lookups;
                            #pragma omp atomic
                            run_metrics.missed_lookups += temp_missed_lookups;
                            #pragma omp atomic
                            run_metrics.total_inserts += temp_inserts;
                            #pragma omp atomic
                            run_metrics.failed_match += temp_failed_match;
                        }
                    }

                    if (end_of_file) {
                        break;
                    }

                    count++;

                    if (count >= MAX_TASK_POOL - 1) {
                        break;
                    }

                    #pragma omp atomic read
                    temp_resize_needed = resize_needed;

                    if (temp_resize_needed) {
                        break;
                    }
                }

                if (!(count == MAX_TASK_POOL - 1) && !(temp_resize_needed)) {
                    end_of_file = 1;
                }
            }

            #pragma omp taskwait

            #pragma omp barrier

            if (resize_needed) {
                resize(&chained);
                resize_needed = 0;
            }

            #pragma omp barrier // VERY MUCH NEEDED
        }
    }

    run_metrics.end = omp_get_wtime();

    printf("execution time: %f seconds\n", run_metrics.end - run_metrics.start);
    if (!speed_test) {
        printf("total_ops: %" PRIu64 "\n", run_metrics.total_ops);
        printf("total_lookups: %" PRIu64 "\n", run_metrics.total_lookups);
        printf("successful_lookups: %" PRIu64 "\n", run_metrics.successful_lookups);
        printf("failed_lookups: %" PRIu64 "\n", run_metrics.missed_lookups);
        printf("total_inserts: %" PRIu64 "\n", run_metrics.total_inserts);
        printf("failed_matches: %" PRIu64 "\n", run_metrics.failed_match);
    }

    destroy_table(chained);
    fclose(f);
}