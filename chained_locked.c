/**
 * @file chained_locked.c
 * @author Ryan Frost (rfrost26@vt.edu)
 * @brief Basic bucketized chained implementation using striped locks
 * @version 0.1
 * @date 2025-12-10
 * 
 * I have decided to transition to a simpler implementation because
 * creating a lock-free chained was turning out to be very difficult.
 * This is an implementation of a chained bucketized hash table,
 * which will hopefully be easier to make lock free with 
 * compare and set.
 * 
 * Chained hash table has a linked list for each bucket
 * rather than an array like chained. Additionally, there is only one
 * hash function because there is no kicking function. The main issue
 * with lock-free chained is the fact that locking two buckets is a
 * core function, which can not be replaced with a specific atomic
 * function like compare and set. There are lock-free chained 
 * implementations, and I will compare the results of this
 * chained implementation to chained implementations shown in a 
 * recent research paper.
 */

#include "chained.h"

#include <omp.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <inttypes.h>
#include <time.h>
#include <unistd.h>
#include <getopt.h>

// Local constants
#define MAX_CHAIN_SIZE 8   // depth of each bucket in the table
#define MAX_TASK_POOL 256

int resize_enabled = 1;
int speed_test = 0;
int resize_needed = 0;

/**
 * @struct Item
 * @brief Item entry in hash table
 * 
 * Essentially represents a "Node" now for the bucket linked list.
 * 
 * @param key uint64_t -> hash table key
 * @param value uint64_t -> item value
 */
typedef struct Item {
    uint64_t key; /** @brief hash table key */
    uint64_t value; /** @brief item value */
    struct Item* next; /** @brief next item in bucket linked list */
} Item;

/**
 * @struct Bucket
 * @brief Bucket at specific hash index
 * 
 * This now has a head pointer rather than an array of items
 * 
 * @param items Item* -> head pointer for linked list
 */
typedef struct {
    Item* head; /** @brief head pointer for linked list */
} Bucket;

typedef struct {
    omp_lock_t lock;
    char padding[64 - sizeof(omp_lock_t)];
} PaddedLock;

/**
 * @struct ChainedHashTable
 * @brief chained hash table
 * 
 * @param buckets Bucket* -> pointer to array of buckets
 * @param num_buckets size_t -> number of buckets
 * @param locks omp_lock_t* -> pointer to array of locks
 * @param num_locks size_t -> number of locks
 * @param num_items volatile int -> number of items in the table (metric purposes)
 */
struct ChainedHashTable{
    Bucket* buckets; /** @brief pointer to array of buckets */
    size_t num_buckets; /** @brief number of buckets */

    PaddedLock* locks; /** @brief pointer to array of locks */
    size_t num_locks; /** @brief number of locks */

    volatile int num_items; /** @brief number of items in the table (metric purposes) */
};

/**
 * @brief hash function
 * 
 * Now the only hash funtion.
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
 * @brief get lock index for specific bucket
 * 
 * This is what implements the "striped" lock.
 * This is the same as chained, a number of buckets have shared locks
 * for memory saving purposes.
 * 
 * @param chained ChainedHashTable* -> specific chained table
 * @param bucket_index size_t -> bucket index in table
 * @return size_t -> lock index
 */
size_t get_lock_idx(ChainedHashTable* chained, size_t bucket_index) {
    return bucket_index % chained->num_locks;
}

/**
 * @brief Create chained hash table
 * 
 * @param num_buckets size_t -> initial number of buckets
 * @param num_locks size_t -> initial number of locks
 * @return ChainedHashTable* 
 */
ChainedHashTable* create_table(size_t num_buckets, size_t num_locks) {
    ChainedHashTable* chained = malloc(sizeof(ChainedHashTable));

    chained->num_buckets = num_buckets;
    chained->num_locks = num_locks;

    chained->num_items = 0; // Metric purposes

    chained->buckets = calloc(num_buckets, sizeof(Bucket));
    chained->locks = malloc(num_locks * sizeof(PaddedLock));

    // omp_init_lock must be used to initialize every lock
    for (size_t i = 0; i < num_locks; i++) {
        omp_init_lock(&chained->locks[i].lock);
    }

    return chained;
}

/**
 * @brief Destroy chained hash table
 * 
 * @param chained ChainedHashTable* -> table to destroy
 */
void destroy_table(ChainedHashTable* chained) {

    // Iterate over all buckets and free each linked list node
    for (size_t i = 0; i < chained->num_buckets; i++) {
        Item* curr = chained->buckets[i].head;
        while (curr != NULL) {
            Item* temp = curr;
            curr = curr->next;
            free(temp);
        }
    }

    // use omp_destroy_lock to remove each lock
    for (size_t i =0; i < chained->num_locks; i++) {
        omp_destroy_lock(&chained->locks[i].lock);
    }

    free(chained->locks);
    free(chained->buckets);
    free(chained);
}

/**
 * @brief lookup key in chained table
 * 
 * @param chained ChainedHashTable -> specific chained table
 * @param key uint64_t -> key value (must not be INVALID_KEY)
 * @return uint64_t -> value at key (INVALID_VALUE if key not found)
 */
uint64_t lookup(ChainedHashTable* chained, uint64_t key) {

    if (key == INVALID_KEY) {
        //printf("key must not equal INVALID_KEY value (uint64 max)");
        return INVALID_VALUE;
    }

    size_t bucket = hash1(key, chained->num_buckets);
    size_t lock_idx = get_lock_idx(chained, bucket);

    uint64_t value = INVALID_VALUE;

    omp_set_lock(&chained->locks[lock_idx].lock);

    Item* curr = chained->buckets[bucket].head;

    while (curr != NULL) {
        if (curr->key == key) {
            value = curr->value;
            break;
        }
        curr = curr->next;
    }

    omp_unset_lock(&chained->locks[lock_idx].lock);

    return value;
}

/**
 * @brief Insert item into chained table
 * 
 * @param chained ChainedHashTable* -> specific chained table
 * @param key uint64_t -> key value (must not be INVALID_KEY)
 * @param value uint64_t -> value value (must not be INVALID_VALUE)
 */
void insert(ChainedHashTable* chained, uint64_t key, uint64_t value) {
    if (key == INVALID_KEY) {
        //printf("key must not equal INVALID_KEY value (uint64 max)");
        return;
    } else if (value == INVALID_VALUE) {
        //printf("value must not equal INVALID_VALUE value (uint64 max)");
        return;
    }

    size_t bucket = hash1(key, chained->num_buckets);
    size_t lock_idx = get_lock_idx(chained, bucket);

    int succeeded = 0;
    int added_node = 0;

    omp_set_lock(&chained->locks[lock_idx].lock);

    // Check if key already exists in the linked list
    Item* curr = chained->buckets[bucket].head;
    int depth = 0;

    while (curr != NULL) {
        if (curr->key == key) {
            curr->value = value;
            succeeded = 1;
            break;
        }
        depth++;
        curr = curr->next;
    }

    if (succeeded) {
        omp_unset_lock(&chained->locks[lock_idx].lock);
        return;
    }

    /* The add section conforms to previous work done on 
    creating a concurrent queue. A compare and set should
    work here for the lock-free version, which is much
    simpler than the previous two bucket situation of the
    cuckoo hash table. */

    // Add new item
    Item* add_item = malloc(sizeof(Item));
    add_item->key = key;
    add_item->value = value;
    add_item->next = chained->buckets[bucket].head;
    chained->buckets[bucket].head = add_item;

    added_node = 1;

    omp_unset_lock(&chained->locks[lock_idx].lock);

    /* Currently debating two design decisions regarding resizing.
    Fixed-size: no need for traking current items, no need for running
    resize function. Obvious performance gains but loses modularity.
    
    Dynamic-size: need some metric to determine when to resize, need
    to periodically resize. High contention on total item counter will
    slow down the parallelism 
    
    I think I will analyze both. I need to find a better way to track
    resizing that doesn't rely on a single counter. */

    if (added_node) {
        int current_items;

        if (!speed_test) {
            #pragma omp atomic capture
            current_items = ++chained->num_items;
        }

        if (resize_enabled && depth >= MAX_CHAIN_SIZE) {
            int temp_resize = 0;

            #pragma omp atomic read
            temp_resize = resize_needed;

            if (!temp_resize) {
                #pragma omp atomic write
                resize_needed = 1;
            }
        }
    }
}

/**
 * @brief Thread safe insert to be used during resize
 * 
 * @param chained ChainedHashTable** -> pointer to chained tabl
 * @param key uint64_t -> key value
 * @param value uint64_t -> value value
 */
void resize_insert(ChainedHashTable* chained, uint64_t key, uint64_t value) {
    size_t bucket = hash1(key, chained->num_buckets);
    size_t lock_idx = get_lock_idx(chained, bucket);

    omp_set_lock(&chained->locks[lock_idx].lock);

    Item* add_item = malloc(sizeof(Item));
    add_item->key = key;
    add_item->value = value;
    add_item->next = chained->buckets[bucket].head;
    chained->buckets[bucket].head = add_item;

    omp_unset_lock(&chained->locks[lock_idx].lock);
}

/**
 * @brief Resize chained table
 * 
 * @param chained_pointer chained table to resize
 */
void resize(ChainedHashTable** chained_pointer) {

    static ChainedHashTable* next_chained = NULL;
    ChainedHashTable* curr_chained = *chained_pointer;

    #pragma omp barrier

    #pragma omp single
    {
        size_t next_num_buckets = curr_chained->num_buckets * 2; // Double size every resize
        size_t next_num_locks = curr_chained->num_locks * 2;
        next_chained = create_table(next_num_buckets, next_num_locks);
        next_chained->num_items = curr_chained->num_items;
    }

    #pragma omp barrier

    #pragma omp for
    for (size_t i = 0; i < curr_chained->num_buckets; i++) {
        Item* curr = curr_chained->buckets[i].head;
        while (curr != NULL) {
            resize_insert(next_chained, curr->key, curr->value);
            curr = curr->next;
        }
    }

    #pragma omp single 
    {
        *chained_pointer = next_chained;
        destroy_table(curr_chained);
        next_chained = NULL;

        #pragma omp atomic write
        resize_needed = 0;
    }

    #pragma omp barrier
}