/**
 * @file chained_lock_free.c
 * @author Ryan Frost (rfrost26@vt.edu)
 * @brief Basic bucketized chained implementation using striped locks
 * @version 0.1
 * @date 2025-12-10
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
 * @brief Create chained hash table
 * 
 * @param num_buckets size_t -> initial number of buckets
 * @param num_locks size_t -> initial number of locks
 * @return ChainedHashTable* 
 */
ChainedHashTable* create_table(size_t num_buckets, size_t num_locks) {
    ChainedHashTable* chained = malloc(sizeof(ChainedHashTable));

    chained->num_buckets = num_buckets;

    chained->num_items = 0; // Metric purposes

    chained->buckets = calloc(num_buckets, sizeof(Bucket));

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

    uint64_t value = INVALID_VALUE;

    Item* curr = chained->buckets[bucket].head;

    while (curr != NULL) {
        if (curr->key == key) {
            #pragma omp atomic read
            value = curr->value;
            return value;
        }
        curr = curr->next;
    }

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

    Item* add_item = malloc(sizeof(Item));
    add_item->key = key;
    add_item->value = value;
    add_item->next = NULL;
    
    int added_node = 0;
    int depth = 0;

    // Check if key already exists in the linked list

    while (1) {
        depth = 0;

        Item* expected = chained->buckets[bucket].head;

        Item* curr = expected;
        int succeeded = 0;

        while (curr != NULL) {
            if (curr->key == key) {
                #pragma omp atomic write
                curr->value = value;
                succeeded = 1;
                break;
            }
            depth++;
            curr = curr->next;
        }

        if (succeeded) {
            free(add_item);
            break;
        }

        add_item->next = expected;

        Item* old_head = NULL;

        #pragma omp atomic compare capture
        {
            old_head = chained->buckets[bucket].head;
            if (chained->buckets[bucket].head == expected) {
                chained->buckets[bucket].head = add_item;
            }
        }

        if (old_head == expected) {
            added_node = 1;
            break;
        }
    }

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

    Item* add_item = malloc(sizeof(Item));
    add_item->key = key;
    add_item->value = value;
    add_item->next = NULL;

    while (1) {
        Item* expected = chained->buckets[bucket].head;
        add_item->next = expected;

        Item* old_head = NULL;

        #pragma omp atomic compare capture
        {
            old_head = chained->buckets[bucket].head;
            if (chained->buckets[bucket].head == expected) {
                chained->buckets[bucket].head = add_item;
            }
        }

        if (old_head == expected) {
            break;
        }
    }
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
        next_chained = create_table(next_num_buckets, 1);
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