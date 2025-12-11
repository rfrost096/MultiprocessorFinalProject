#include <stdio.h>
#include <omp.h>

int main() {

    /* OpenMP uses all available cores as default. My laptop has 16 cores.
    This can be changed with OMP_NUM_THREADS (found in run.sh)
    From the docs:
    OMP_NUM_THREADS list [4.1.3] [21.1.2]
        Sets the initial value of the nthreads-var ICV for the
        number of threads to use for parallel regions.
    */

    #pragma omp parallel
    {
        int thread_id = omp_get_thread_num();

        int total_threads= omp_get_num_threads();

        printf("thread %d/%d\n", thread_id, total_threads);
    }
}