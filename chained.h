/**
 * @file chained.h
 * @author Ryan Frost (rfrost26@vt.edu)
 * @brief header file
 * @version 0.1
 * @date 2025-12-10
 */

#ifndef CHAINED_H
#define CHAINED_H

#include <stdint.h>
#include <stddef.h>
#include <omp.h>

// Global Constants
#define INVALID_KEY UINT64_MAX
#define INVALID_VALUE UINT64_MAX
#define DEFAULT_NUM_THREADS 16

// Global config flags
extern int resize_enabled;
extern int speed_test;
extern int resize_needed;

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
typedef struct ChainedHashTable ChainedHashTable;

/**
 * @brief Create chained hash table
 * 
 * @param num_buckets size_t -> initial number of buckets
 * @param num_locks size_t -> initial number of locks
 * @return ChainedHashTable* 
 */
ChainedHashTable* create_table(size_t num_buckets, size_t num_locks);

/**
 * @brief Destroy chained hash table
 * 
 * @param chained ChainedHashTable* -> table to destroy
 */
void destroy_table(ChainedHashTable* chained);

/**
 * @brief lookup key in chained table
 * 
 * @param chained ChainedHashTable -> specific chained table
 * @param key uint64_t -> key value (must not be INVALID_KEY)
 * @return uint64_t -> value at key (INVALID_VALUE if key not found)
 */
uint64_t lookup(ChainedHashTable* chained, uint64_t key);

/**
 * @brief Insert item into chained table
 * 
 * @param chained ChainedHashTable -> specific chained table
 * @param key uint64_t -> key value (must not be INVALID_KEY)
 * @param value uint64_t -> value value (must not be INVALID_VALUE)
 */
void insert(ChainedHashTable* chained, uint64_t key, uint64_t value);

/**
 * @brief Resize chained table
 * 
 * @param chained_pointer chained table to resize
 */
void resize(ChainedHashTable** chained_pointer);

#endif // CHAINED_H