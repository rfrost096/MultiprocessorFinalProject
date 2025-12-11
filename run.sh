gcc -fopenmp main.c chained_locked.c -o chained_locked.exe
gcc -fopenmp main.c chained_lock_free.c -o chained_lock_free.exe

echo "scalability test"

echo "lock-based"

./chained_locked.exe -f datasets/write_heavy.txt -t 1 -s -b 64 -r
./chained_locked.exe -f datasets/write_heavy.txt -t 2 -s -b 64 -r
./chained_locked.exe -f datasets/write_heavy.txt -t 4 -s -b 64 -r
./chained_locked.exe -f datasets/write_heavy.txt -t 8 -s -b 64 -r
./chained_locked.exe -f datasets/write_heavy.txt -t 12 -s -b 64 -r

echo "lock-free"

./chained_lock_free.exe -f datasets/write_heavy.txt -t 1 -s -b 64 -r
./chained_lock_free.exe -f datasets/write_heavy.txt -t 2 -s -b 64 -r
./chained_lock_free.exe -f datasets/write_heavy.txt -t 4 -s -b 64 -r
./chained_lock_free.exe -f datasets/write_heavy.txt -t 8 -s -b 64 -r
./chained_lock_free.exe -f datasets/write_heavy.txt -t 12 -s -b 64 -r

echo "lock-based"

./chained_locked.exe -f datasets/read_heavy.txt -t 1 -s -b 64 -r
./chained_locked.exe -f datasets/read_heavy.txt -t 2 -s -b 64 -r
./chained_locked.exe -f datasets/read_heavy.txt -t 4 -s -b 64 -r
./chained_locked.exe -f datasets/read_heavy.txt -t 8 -s -b 64 -r
./chained_locked.exe -f datasets/read_heavy.txt -t 12 -s -b 64 -r

echo "lock-free"

./chained_lock_free.exe -f datasets/read_heavy.txt -t 1 -s -b 64 -r
./chained_lock_free.exe -f datasets/read_heavy.txt -t 2 -s -b 64 -r
./chained_lock_free.exe -f datasets/read_heavy.txt -t 4 -s -b 64 -r
./chained_lock_free.exe -f datasets/read_heavy.txt -t 8 -s -b 64 -r
./chained_lock_free.exe -f datasets/read_heavy.txt -t 12 -s -b 64 -r