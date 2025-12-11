# Multiprocessor Programming Final Project

## Bucketized Linked-List Hash Table How to Run

Run "run.sh" to run specific configurations

Compile command:
gcc -fopenmp main.c chained_locked.c -o chained_locked.exe
gcc -fopenmp main.c chained_lock_free.c -o chained_lock_free.exe

Options:
-f -> Data file path
-b -> Initial number of buckets (hash table size)
-t -> Number of threads
-r -> Disable resizing
-s -> Disable metric tracking for speed test

Already generated data is in "datasets"

### Data Generation

Generate data using python_data_generator.py

Multiple configuration options and examples available

### Graph Generation

Manually put in speed test results into generate_graphs.py to generate graphs for the specific configuration you want to see.