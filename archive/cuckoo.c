/**
 * @file cuckoo.c
 * @author Ryan Frost (rfrost26@vt.edu)
 * @brief Basic bucketized cuckoo implementation using striped locks
 * @version 0.1
 * @date 2025-12-10
 * 
 * Cuckoo hashing is a method of dealing with hash table collisions.
 * Each hash key has two possible locations for storage. If the first
 * location is taken, it checks the second one. If both are taken,
 * it randomly "kicks" an item from one of the possible locations,
 * fills that location with this item, and then tries to insert the
 * kicked item. The hope is that the kicked item's other location is
 * available, in which case only two total insertions occured.
 * If the second item's possible locations are also full, then it
 * kicks one location randomly and the process repeats. This technically
 * could happen indefinitely, but we can also create a limit to the
 * number of iterations to prevent getting stuck.
 * 
 * Bucketized cuckoo hashing changes the standard 1D array of key/values 
 * into an array of buckets, each of which can hold multiple key/values.
 * This decreases the chain of kicking because now instead of checking
 * for only two possible locations, we are now checking two possible
 * buckets (which might have something like 4 possible entries).
 * If neither possible buckets have a spot, a random item is kicked from
 * a randomly selected bucket and a similar process as described before
 * takes place.
 * 
 * The implementation in this file uses a striped lock to provide mutual
 * exclusion to the buckets. Rather than having a lock for each specific
 * bucket, which could take up a lot of memory for really large tables,
 * we assign one lock for multiple buckets in a striped fashion.
 * Basically, a bucket's lock is in an array of locks where the index
 * is bucket_index % lock_array_size. This obviously means there may be
 * contentention over a lock when two threads try to access DIFFERENT
 * buckets, so this design decision can reduce performance to 
 * save memory.
 * 
 * References:
 * https://doi.org/10.1007/978-3-031-39698-4_19
 * https://en.wikipedia.org/wiki/Cuckoo_hashing
 */
#include <omp.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>

// Constants
#define NUM_THREADS 16

#define BUCKET_SIZE 4   // depth of each bucket in the table
#define INVALID_KEY UINT64_MAX
#define INVALID_VALUE UINT64_MAX
#define MAX_KICKS 256   // max nuber of kicks before we give up and resize
#define MAX_RECOVERY_QUEUE NUM_THREADS
#define MAX_TASK_POOL 256

/**
 * @struct Item
 * @brief Item entry in hash table
 * 
 * @param key uint64_t -> hash table key
 * @param value uint64_t -> item value
 */
typedef struct {
    uint64_t key; /** @brief hash table key */
    uint64_t value; /** @brief item value */
} Item;

Item* recovery_inserts[MAX_RECOVERY_QUEUE];
int recovery_count = 0;
int resize_needed = 0;
int end_of_file = 0;


typedef struct {
    Item item;
    char padding[64 - sizeof(Item)]; // adding this to remove false sharing
} ThreadItem;

ThreadItem thread_items[NUM_THREADS];

int random_seeds[NUM_THREADS];

/**
 * @struct Bucket
 * @brief Bucket at specific hash index
 * 
 * @param items Item[] -> stored items
 */
typedef struct {
    Item items[BUCKET_SIZE] /** @brief stored items */
} Bucket;

/**
 * @struct CuckooHashTable
 * @brief cuckoo hash table
 * 
 * @param buckets Bucket* -> pointer to array of buckets
 * @param num_buckets size_t -> number of buckets
 * @param locks omp_lock_t* -> pointer to array of locks
 * @param num_locks size_t -> number of locks
 */
typedef struct {
    Bucket* buckets; /** @brief pointer to array of buckets */
    size_t num_buckets; /** @brief number of buckets */

    omp_lock_t* locks; /** @brief pointer to array of locks */
    size_t num_locks; /** @brief number of locks */
} CuckooHashTable;

/**
 * @brief first hash function
 * 
 * @param key uint64_t -> hash table key
 * @param num_buckets size_t -> number of hash table buckets
 * @return size_t -> hash1 index
 */
size_t hash1(uint64_t key, size_t num_buckets) {
    key = (key * 37) + 13;
    return key % num_buckets;
}

/**
 * @brief second hash function
 * 
 * @param key uint64_t -> hash table key
 * @param num_buckets size_t -> number of hash table buckets
 * @return size_t -> hash2 index
 */
size_t hash2(uint64_t key, size_t num_buckets) {
    key = (key * 31) + 11;
    return key % num_buckets;
}

/**
 * @brief get lock index for specific bucket
 * 
 * @param cuckoo CuckooHashTable* -> specific cuckoo table
 * @param bucket_index size_t -> bucket index in table
 * @return size_t -> lock index
 */
size_t get_lock_idx(CuckooHashTable* cuckoo, size_t bucket_index) {
    return bucket_index % cuckoo->num_locks;
}

/**
 * @brief lock two buckets
 * 
 * These are the two buckets that are possible for a specific key
 * 
 * @param cuckoo CuckooHashTable* -> specific cuckoo table
 * @param first_bucket size_t -> first possible bucket
 * @param second_bucket size_t -> second possible bucket
 */
void lock_two_buckets(CuckooHashTable* cuckoo, size_t first_bucket, size_t second_bucket) {
    size_t first_lock = get_lock_idx(cuckoo, first_bucket);
    size_t second_lock = get_lock_idx(cuckoo, second_bucket);

    if (first_lock == second_lock) {
        // If the locks are the same, just acquire that lock
        omp_set_lock(&cuckoo->locks[first_lock]);
    } else if (first_lock < second_lock) {
        /* It is possible that two threads may want the same two locks.
        If Thread A wants lock 2 and lock 5, and Thread B wants them too,
        there is a possibility that Thread A locks lock 2 and Thread B
        locks lock 5 and then they are stuck. To prevent this, I have just
        requried that they lock them in order. Someone will lock lock 2 
        first, and then try to lock lock 5 while the other waits for lock 2. */

        omp_set_lock(&cuckoo->locks[first_lock]);
        omp_set_lock(&cuckoo->locks[second_lock]);
    } else {
        omp_set_lock(&cuckoo->locks[second_lock]);
        omp_set_lock(&cuckoo->locks[first_lock]);
    }
}

/**
 * @brief unlock two buckets
 * 
 * @param cuckoo CuckooHashTable* -> specific cuckoo table
 * @param first_bucket size_t -> first possible bucket
 * @param second_bucket size_t -> second possible bucket
 */
void unlock_two_buckets(CuckooHashTable* cuckoo, size_t first_bucket, size_t second_bucket) {
    size_t first_lock = get_lock_idx(cuckoo, first_bucket);
    size_t second_lock = get_lock_idx(cuckoo, second_bucket);

    omp_unset_lock(&cuckoo->locks[first_lock]);
    omp_unset_lock(&cuckoo->locks[second_lock]);
}

/**
 * @brief Create cuckoo hash table
 * 
 * @param num_buckets size_t -> initial number of buckets
 * @param num_locks size_t -> initial number of locks
 * @return CuckooHashTable* 
 */
CuckooHashTable* create_table(size_t num_buckets, size_t num_locks) {
    CuckooHashTable* cuckoo = malloc(sizeof(CuckooHashTable));

    cuckoo->num_buckets = num_buckets;
    cuckoo->num_locks = num_locks;

    cuckoo->buckets = calloc(num_buckets, sizeof(Bucket));
    cuckoo->locks = calloc(num_locks, sizeof(omp_lock_t));

    // omp_init_lock must be used to initialize every lock
    for (size_t i = 0; i < num_locks; i++) {
        omp_init_lock(&cuckoo->locks[i]);
    }

    // initializing to signify not currently occupied
    for (size_t i = 0; i < num_buckets; i++) {
        for (size_t j = 0; j < BUCKET_SIZE; j++) {
            cuckoo->buckets[i].items[j].key = UINT64_MAX;
            cuckoo->buckets[i].items[j].value = UINT64_MAX;
        }
    }

    return cuckoo;
}

/**
 * @brief Destroy cuckoo hash table
 * 
 * @param cuckoo CuckooHashTable* -> table to destroy
 */
void destroy_table(CuckooHashTable* cuckoo) {
    // use omp_destroy_lock to remove each lock
    for (size_t i =0; i < cuckoo->num_locks; i++) {
        omp_destroy_lock(&cuckoo->locks[i]);
    }

    free(cuckoo->locks);
    free(cuckoo->buckets);
    free(cuckoo);
}

/**
 * @brief lookup key in cuckoo table
 * 
 * @param cuckoo CuckooHashTable -> specific cuckoo table
 * @param key uint64_t -> key value
 * @return uint64_t -> value at key
 */
uint64_t lookup(CuckooHashTable* cuckoo, uint64_t key) {

    if (key == INVALID_KEY) {
        //printf("key must not equal INVALID_KEY value (uint64 max)");
        return INVALID_VALUE;
    }

    size_t first_bucket = hash1(key, cuckoo->num_buckets);
    size_t second_bucket = hash2(key, cuckoo->num_buckets);

    uint64_t value = INVALID_VALUE;

    lock_two_buckets(cuckoo, first_bucket, second_bucket);

    for (int i = 0; i < BUCKET_SIZE; i++) {
        if (key == cuckoo->buckets[first_bucket].items[i].key) {
            value = cuckoo->buckets[first_bucket].items[i].value;
        }
    }

    for (int i = 0; i < BUCKET_SIZE; i++) {
        if (key == cuckoo->buckets[second_bucket].items[i].key) {
            value = cuckoo->buckets[second_bucket].items[i].value;
        }
    }

    if (value != INVALID_VALUE) {
        unlock_two_buckets(cuckoo, first_bucket, second_bucket);
        return value;
    }

    for (int i = 0; i < NUM_THREADS; i++) {
        uint64_t temp_key;
        uint64_t temp_value;
        
        #pragma omp atomic read
        temp_key = thread_items[i].item.key;
        
        if (temp_key == key) {
            #pragma omp atomic read
            temp_value = thread_items[i].item.value;
            unlock_two_buckets(cuckoo, first_bucket, second_bucket);
            return temp_value;
        }
    }

    unlock_two_buckets(cuckoo, first_bucket, second_bucket);
    return value;
}

void resize(CuckooHashTable* cuckoo) {

}

void clear_thread_item(int thread_id) {
    #pragma omp atomic write
    thread_items[thread_id].item.key = INVALID_KEY;
    #pragma omp atomic write
    thread_items[thread_id].item.value = INVALID_VALUE;
}

void set_thread_item(int thread_id, uint64_t key, uint64_t value) {
    #pragma omp atomic write
    thread_items[thread_id].item.key = key;
    #pragma omp atomic write
    thread_items[thread_id].item.value = value;
}

void insert(CuckooHashTable* cuckoo, uint64_t key, uint64_t value) {
    if (key == INVALID_KEY) {
        //printf("key must not equal INVALID_KEY value (uint64 max)");
        return;
    } else if (value == INVALID_VALUE) {
        //printf("value must not equal INVALID_VALUE value (uint64 max)");
        return;
    }

    uint64_t curr_key = key;
    uint64_t curr_value = value;
    int thread_id = omp_get_thread_num();
    int succeeded = 0;

    clear_thread_item(thread_id);

    for (int i = 0; i < MAX_KICKS; i++) {
        size_t first_bucket = hash1(curr_key, cuckoo->num_buckets);
        size_t second_bucket = hash2(curr_key, cuckoo->num_buckets);

        lock_two_buckets(cuckoo, first_bucket, second_bucket);

        for (size_t i = 0; i < BUCKET_SIZE; i++) {
            if (cuckoo->buckets[first_bucket].items[i].key == curr_key) {
                cuckoo->buckets[first_bucket].items[i].value = curr_value;
                succeeded = 1;
                break;
            }
        }

        if (succeeded) {
            clear_thread_item(thread_id);
            unlock_two_buckets(cuckoo, first_bucket, second_bucket);
            break;
        }

        for (size_t i = 0; i < BUCKET_SIZE; i++) {
            if (cuckoo->buckets[second_bucket].items[i].key == curr_key) {
                cuckoo->buckets[second_bucket].items[i].value = curr_value;
                succeeded = 1;
                break;
            }
        }

        if (succeeded) {
            clear_thread_item(thread_id);
            unlock_two_buckets(cuckoo, first_bucket, second_bucket);
            break;
        }

        for (int i = 0; i < NUM_THREADS; i++) {
            uint64_t temp_key;
            
            #pragma omp atomic read
            temp_key = thread_items[i].item.key;
            
            if (temp_key == curr_key) {
                #pragma omp atomic write
                thread_items[i].item.value = curr_value;

                succeeded = 1;
                break;
            }
        }

        if (succeeded) {
            clear_thread_item(thread_id);
            unlock_two_buckets(cuckoo, first_bucket, second_bucket);
            break;
        }

        for (size_t i = 0; i < BUCKET_SIZE; i++) {
            if (cuckoo->buckets[first_bucket].items[i].key == curr_key) {
                cuckoo->buckets[first_bucket].items[i].key = curr_key;
                cuckoo->buckets[first_bucket].items[i].value = curr_value;
                succeeded = 1;
                break;
            }
        }

        if (succeeded) {
            clear_thread_item(thread_id);
            unlock_two_buckets(cuckoo, first_bucket, second_bucket);
            break;
        }

        for (size_t i = 0; i < BUCKET_SIZE; i++) {
            if (cuckoo->buckets[second_bucket].items[i].key == INVALID_KEY) {
                cuckoo->buckets[second_bucket].items[i].key = curr_key;
                cuckoo->buckets[second_bucket].items[i].value = curr_value;
                succeeded = 1;
                break;
            }
        }

        if (succeeded) {
            clear_thread_item(thread_id);
            unlock_two_buckets(cuckoo, first_bucket, second_bucket);
            break;
        }

        int random_kick = rand_r(random_seeds[thread_id]) % (2 * BUCKET_SIZE);
        size_t random_bucket;
        size_t random_item;

        if (random_kick < BUCKET_SIZE) {
            random_bucket = first_bucket;
            random_item = random_kick;
        } else {
            random_bucket = second_bucket;
            random_item = random_kick % BUCKET_SIZE;
        }

        uint64_t kick_key = cuckoo->buckets[random_bucket].items[random_item].key;
        uint64_t kick_value = cuckoo->buckets[random_bucket].items[random_item].value;

        set_thread_item(thread_id, kick_key, kick_value);

        cuckoo->buckets[random_bucket].items[random_item].key = curr_key;
        cuckoo->buckets[random_bucket].items[random_item].value = curr_value;

        curr_key = kick_key;
        curr_value = kick_value;

        unlock_two_buckets(cuckoo, first_bucket, second_bucket);
    }

    if (succeeded) {
        return;
    }

    #pragma omp critical
    int idx;
    #pragma omp atomic capture
    idx = recovery_count++;

    Item* item = malloc(sizeof(Item));

    item->key = curr_key;
    item->value = curr_value;
    
    recovery_inserts[idx] = item;

    #pragma omp atomic write
    resize_needed = 1;
}

#define MAX_LINE_LENGTH 256
#define INIT_NUM_BUCKETS 64
#define INIT_NUM_LOCKS 8

typedef struct {
    FILE *file;
    char hash_op;
    uint64_t key;
    uint64_t value;
} FileIterator;

int next_record(FileIterator *iter) {
    char line[MAX_LINE_LENGTH];

    if (fgets(line, MAX_LINE_LENGTH, iter->file) == NULL) {
        return 0;
    }

    sscanf(line, " %c %" PRIu64 " %" PRIu64, &iter->hash_op, &iter->key, &iter->value);

    return 1;
}

int main() {
    char hash_op;
    uint64_t key;
    uint64_t value;
    char line[MAX_LINE_LENGTH];

    FILE *f = fopen("data.txt", "r");
    if (f == NULL) return 1;

    FileIterator iter = { .file = f };

    CuckooHashTable* cuckoo = create_table(INIT_NUM_BUCKETS, INIT_NUM_LOCKS);

    for (int i = 0; i < NUM_THREADS; i++) {
        random_seeds[i] = i * 31 + (unsigned int)time(NULL);
    }

    #pragma omp parallel
    {
        while (!end_of_file) {

            #pragma omp single
            {
                int temp_resize_needed;
                int count = 0;
                while (next_record(&iter)) {
                    uint64_t key = iter.key;
                    uint64_t value = iter.value;

                    // Lookup
                    if (iter.hash_op == 'L') {
                        #pragma omp task firstprivate(key)
                        {
                            lookup(cuckoo, key);
                        }
                    } 
                    // Insert
                    else if (iter.hash_op == 'I') {
                        #pragma omp task firstprivate(key, value)
                        {
                            insert(cuckoo, key, value);
                        }
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

                if (!(count >= MAX_TASK_POOL) && !(temp_resize_needed)) {
                    end_of_file = 1;
                }

                #pragma omp taskwait
            }

            if (resize_needed) {
                resize(cuckoo);

                #pragma omp for
                for (int i = 0; i < recovery_count; i++) {
                    insert(cuckoo, recovery_inserts[i]->key, recovery_inserts[i]->value);
                    free(recovery_inserts[i]);
                }

                #pragma omp single
                {
                    recovery_count = 0;
                    
                    #pragma omp atomic write
                    {
                        resize_needed = 0;
                    }
                }
            }
        }
    }

    destroy_table(cuckoo);
}