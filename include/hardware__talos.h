// Hardware information for my Talos machine (AMD Ryzen 9900X CPU 16-Core Processor).

// Architecture from `uname -srm`.
#define SPC__X86_64

// CPU from `/proc/cpuinfo`.
#define SPC__CPU_NAME "AMD Ryzen 9 9950X 16-Core Processor"

// AMD Ryzen 9 9950X is single node and has 16 cores and 32 threads.
#define SPC__CORE_COUNT                     16
#define SPC__THREAD_COUNT                   32
#define SPC__NUMA_NODE_COUNT                1
#define SPC__NUMA_NODES_ACTIVE_IN_BENCHMARK 1

// Main memory per NUMA node (MB).
#define SPC__NUMA_NODE_DRAM_MB 61884

// Obtained from `lsb_release -a`.
#define SPC__OS "Fedora Linux 41 (KDE Plasma)"

// Obtained from: `uname -srm`.
#define SPC__KERNEL "Linux 6.13.5-200.fc41.x86_64 x86_64"

// AMD: possible options are AVX, AVX2, and AVX512.
#define SPC__SUPPORTS_AVX

// Cache information from `getconf -a | grep CACHE`.
#define SPC__LEVEL1_ICACHE_SIZE 32768
#define SPC__LEVEL1_ICACHE_ASSOC
#define SPC__LEVEL1_ICACHE_LINESIZE 64
#define SPC__LEVEL1_DCACHE_SIZE     49152
#define SPC__LEVEL1_DCACHE_ASSOC    12
#define SPC__LEVEL1_DCACHE_LINESIZE 64
#define SPC__LEVEL2_CACHE_SIZE      1048576
#define SPC__LEVEL2_CACHE_ASSOC     16
#define SPC__LEVEL2_CACHE_LINESIZE  64
#define SPC__LEVEL3_CACHE_SIZE      33554432
#define SPC__LEVEL3_CACHE_ASSOC     16
#define SPC__LEVEL3_CACHE_LINESIZE  64
#define SPC__LEVEL4_CACHE_SIZE      0
#define SPC__LEVEL4_CACHE_ASSOC
#define SPC__LEVEL4_CACHE_LINESIZE
