# SIGMOD Contest 2025 (Solved)

This repository contains my solutions for the SIGMOD Contest 2025.

## Changes

This is a list of changes (divergences) from the main challenge repository.

- Add hardware definition file for the machine I used.
- Running `clang-format` on all files.
- Fixed issue due to a missing include for `hardware.h` (replaced by `hardware__talos.h`).

## Baseline

```
$ time ./build/run plans.json
Query 1a >>              Runtime: 1837 ms - Result correct: true
Query 1b >>              Runtime: 963 ms - Result correct: true
Query 1c >>              Runtime: 354 ms - Result correct: true
Query 1d >>              Runtime: 1374 ms - Result correct: true
Query 2a >>              Runtime: 2593 ms - Result correct: true
Query 2b >>              Runtime: 2383 ms - Result correct: true
Query 2c >>              Runtime: 2315 ms - Result correct: true
Query 2d >>              Runtime: 2842 ms - Result correct: true
Query 3a >>              Runtime: 1255 ms - Result correct: true
Query 3b >>              Runtime: 816 ms - Result correct: true
Query 3c >>              Runtime: 1658 ms - Result correct: true
Query 4a >>              Runtime: 1434 ms - Result correct: true
Query 4b >>              Runtime: 858 ms - Result correct: true
Query 4c >>              Runtime: 1767 ms - Result correct: true
Query 5a >>              Runtime: 543 ms - Result correct: true
Query 5b >>              Runtime: 341 ms - Result correct: true
Query 5c >>              Runtime: 1234 ms - Result correct: true
Query 6a >>              Runtime: 14609 ms - Result correct: true
Query 6b >>              Runtime: 12257 ms - Result correct: true
Query 6c >>              Runtime: 12120 ms - Result correct: true
Query 6d >>              Runtime: 13744 ms - Result correct: true
Query 6e >>              Runtime: 13765 ms - Result correct: true
Query 6f >>              Runtime: 15426 ms - Result correct: true
Query 7a >>              Runtime: 13434 ms - Result correct: true
Query 7b >>              Runtime: 11958 ms - Result correct: true
Query 7c >>              Runtime: 13523 ms - Result correct: true
Query 8a >>              Runtime: 2342 ms - Result correct: true
Query 8b >>              Runtime: 651 ms - Result correct: true
Query 8c >>              Runtime: 16555 ms - Result correct: true
Query 8d >>              Runtime: 15376 ms - Result correct: true
Query 9a >>              Runtime: 2771 ms - Result correct: true
Query 9b >>              Runtime: 2303 ms - Result correct: true
Query 9c >>              Runtime: 4154 ms - Result correct: true
Query 9d >>              Runtime: 4902 ms - Result correct: true
Query 10a >>             Runtime: 2480 ms - Result correct: true
Query 10b >>             Runtime: 2673 ms - Result correct: true
Query 10c >>             Runtime: 3619 ms - Result correct: true
Query 11a >>             Runtime: 1574 ms - Result correct: true
Query 11b >>             Runtime: 888 ms - Result correct: true
Query 11c >>             Runtime: 2394 ms - Result correct: true
Query 11d >>             Runtime: 2757 ms - Result correct: true
Query 12a >>             Runtime: 999 ms - Result correct: true
Query 12b >>             Runtime: 5668 ms - Result correct: true
Query 12c >>             Runtime: 1567 ms - Result correct: true
Query 13a >>             Runtime: 7351 ms - Result correct: true
Query 13b >>             Runtime: 5019 ms - Result correct: true
Query 13c >>             Runtime: 5113 ms - Result correct: true
Query 13d >>             Runtime: 8276 ms - Result correct: true
Query 14a >>             Runtime: 1252 ms - Result correct: true
Query 14b >>             Runtime: 935 ms - Result correct: true
Query 14c >>             Runtime: 1819 ms - Result correct: true
Query 15a >>             Runtime: 1990 ms - Result correct: true
Query 15b >>             Runtime: 1445 ms - Result correct: true
Query 15c >>             Runtime: 2683 ms - Result correct: true
Query 15d >>             Runtime: 2726 ms - Result correct: true
Query 16a >>             Runtime: 14995 ms - Result correct: true
Query 16b >>             Runtime: 17242 ms - Result correct: true
Query 16c >>             Runtime: 14447 ms - Result correct: true
Query 16d >>             Runtime: 14539 ms - Result correct: true
Query 17a >>             Runtime: 14593 ms - Result correct: true
Query 17b >>             Runtime: 14245 ms - Result correct: true
Query 17c >>             Runtime: 13983 ms - Result correct: true
Query 17d >>             Runtime: 13948 ms - Result correct: true
Query 17e >>             Runtime: 15813 ms - Result correct: true
Query 17f >>             Runtime: 14637 ms - Result correct: true
Query 18a >>             Runtime: 7003 ms - Result correct: true
Query 18b >>             Runtime: 1503 ms - Result correct: true
Query 18c >>             Runtime: 2868 ms - Result correct: true
Query 19a >>             Runtime: 2643 ms - Result correct: true
Query 19b >>             Runtime: 1890 ms - Result correct: true
Query 19c >>             Runtime: 3405 ms - Result correct: true
Query 19d >>             Runtime: 8991 ms - Result correct: true
Query 20a >>             Runtime: 14085 ms - Result correct: true
Query 20b >>             Runtime: 12573 ms - Result correct: true
Query 20c >>             Runtime: 14521 ms - Result correct: true
Query 21a >>             Runtime: 2195 ms - Result correct: true
Query 21b >>             Runtime: 1721 ms - Result correct: true
Query 21c >>             Runtime: 2156 ms - Result correct: true
Query 22a >>             Runtime: 1800 ms - Result correct: true
Query 22b >>             Runtime: 1665 ms - Result correct: true
Query 22c >>             Runtime: 1994 ms - Result correct: true
Query 22d >>             Runtime: 2303 ms - Result correct: true
Query 23a >>             Runtime: 2162 ms - Result correct: true
Query 23b >>             Runtime: 2215 ms - Result correct: true
Query 23c >>             Runtime: 2387 ms - Result correct: true
Query 24a >>             Runtime: 3494 ms - Result correct: true
Query 24b >>             Runtime: 3210 ms - Result correct: true
Query 25a >>             Runtime: 3093 ms - Result correct: true
Query 25b >>             Runtime: 2062 ms - Result correct: true
Query 25c >>             Runtime: 3334 ms - Result correct: true
Query 26a >>             Runtime: 14454 ms - Result correct: true
Query 26b >>             Runtime: 14347 ms - Result correct: true
Query 26c >>             Runtime: 15066 ms - Result correct: true
Query 27a >>             Runtime: 2135 ms - Result correct: true
Query 27b >>             Runtime: 857 ms - Result correct: true
Query 27c >>             Runtime: 2141 ms - Result correct: true
Query 28a >>             Runtime: 2214 ms - Result correct: true
Query 28b >>             Runtime: 1547 ms - Result correct: true
Query 28c >>             Runtime: 1898 ms - Result correct: true
Query 29a >>             Runtime: 3340 ms - Result correct: true
Query 29b >>             Runtime: 3371 ms - Result correct: true
Query 29c >>             Runtime: 4896 ms - Result correct: true
Query 30a >>             Runtime: 2837 ms - Result correct: true
Query 30b >>             Runtime: 1989 ms - Result correct: true
Query 30c >>             Runtime: 3487 ms - Result correct: true
Query 31a >>             Runtime: 3630 ms - Result correct: true
Query 31b >>             Runtime: 1943 ms - Result correct: true
Query 31c >>             Runtime: 4906 ms - Result correct: true
Query 32a >>             Runtime: 3207 ms - Result correct: true
Query 32b >>             Runtime: 3278 ms - Result correct: true
Query 33a >>             Runtime: 3654 ms - Result correct: true
Query 33b >>             Runtime: 3016 ms - Result correct: true
Query 33c >>             Runtime: 4185 ms - Result correct: true

real    11m7.193s
user    12m19.108s
sys     2m47.299s
```

Most of the low hanging fruits performance wise are to exploit columnar format and use
a parallel hash join in this case `std::unordered_map` scores a 20 second total runtime
replacing it with an off the shelf `flat_hash_map` implementation Abseil-like gets us down
to 18 seconds on the same machine.

```
clflushopt@workstation:~/Projects/Experiments/sigmod-2025$ ./build/run plans.json
Query 1a >>              Runtime: 97 ms - Result correct: true
Query 1b >>              Runtime: 72 ms - Result correct: true
Query 1c >>              Runtime: 13 ms - Result correct: true
Query 1d >>              Runtime: 92 ms - Result correct: true
Query 2a >>              Runtime: 98 ms - Result correct: true
Query 2b >>              Runtime: 107 ms - Result correct: true
Query 2c >>              Runtime: 88 ms - Result correct: true
Query 2d >>              Runtime: 103 ms - Result correct: true
Query 3a >>              Runtime: 41 ms - Result correct: true
Query 3b >>              Runtime: 17 ms - Result correct: true
Query 3c >>              Runtime: 56 ms - Result correct: true
Query 4a >>              Runtime: 50 ms - Result correct: true
Query 4b >>              Runtime: 17 ms - Result correct: true
Query 4c >>              Runtime: 71 ms - Result correct: true
Query 5a >>              Runtime: 23 ms - Result correct: true
Query 5b >>              Runtime: 10 ms - Result correct: true
Query 5c >>              Runtime: 46 ms - Result correct: true
Query 6a >>              Runtime: 130 ms - Result correct: true
Query 6b >>              Runtime: 121 ms - Result correct: true
Query 6c >>              Runtime: 116 ms - Result correct: true
Query 6d >>              Runtime: 156 ms - Result correct: true
Query 6e >>              Runtime: 152 ms - Result correct: true
Query 6f >>              Runtime: 352 ms - Result correct: true
Query 7a >>              Runtime: 482 ms - Result correct: true
Query 7b >>              Runtime: 192 ms - Result correct: true
Query 7c >>              Runtime: 922 ms - Result correct: true
Query 8a >>              Runtime: 87 ms - Result correct: true
Query 8b >>              Runtime: 25 ms - Result correct: true
Query 8c >>              Runtime: 631 ms - Result correct: true
Query 8d >>              Runtime: 431 ms - Result correct: true
Query 9a >>              Runtime: 141 ms - Result correct: true
Query 9b >>              Runtime: 112 ms - Result correct: true
Query 9c >>              Runtime: 336 ms - Result correct: true
Query 9d >>              Runtime: 479 ms - Result correct: true
Query 10a >>             Runtime: 100 ms - Result correct: true
Query 10b >>             Runtime: 228 ms - Result correct: true
Query 10c >>             Runtime: 354 ms - Result correct: true
Query 11a >>             Runtime: 56 ms - Result correct: true
Query 11b >>             Runtime: 16 ms - Result correct: true
Query 11c >>             Runtime: 127 ms - Result correct: true
Query 11d >>             Runtime: 121 ms - Result correct: true
Query 12a >>             Runtime: 26 ms - Result correct: true
Query 12b >>             Runtime: 608 ms - Result correct: true
Query 12c >>             Runtime: 52 ms - Result correct: true
Query 13a >>             Runtime: 810 ms - Result correct: true
Query 13b >>             Runtime: 66 ms - Result correct: true
Query 13c >>             Runtime: 67 ms - Result correct: true
Query 13d >>             Runtime: 705 ms - Result correct: true
Query 14a >>             Runtime: 55 ms - Result correct: true
Query 14b >>             Runtime: 17 ms - Result correct: true
Query 14c >>             Runtime: 61 ms - Result correct: true
Query 15a >>             Runtime: 45 ms - Result correct: true
Query 15b >>             Runtime: 26 ms - Result correct: true
Query 15c >>             Runtime: 62 ms - Result correct: true
Query 15d >>             Runtime: 75 ms - Result correct: true
Query 16a >>             Runtime: 238 ms - Result correct: true
Query 16b >>             Runtime: 631 ms - Result correct: true
Query 16c >>             Runtime: 158 ms - Result correct: true
Query 16d >>             Runtime: 192 ms - Result correct: true
Query 17a >>             Runtime: 320 ms - Result correct: true
Query 17b >>             Runtime: 131 ms - Result correct: true
Query 17c >>             Runtime: 76 ms - Result correct: true
Query 17d >>             Runtime: 85 ms - Result correct: true
Query 17e >>             Runtime: 276 ms - Result correct: true
Query 17f >>             Runtime: 252 ms - Result correct: true
Query 18a >>             Runtime: 654 ms - Result correct: true
Query 18b >>             Runtime: 32 ms - Result correct: true
Query 18c >>             Runtime: 131 ms - Result correct: true
Query 19a >>             Runtime: 26 ms - Result correct: true
Query 19b >>             Runtime: 10 ms - Result correct: true
Query 19c >>             Runtime: 53 ms - Result correct: true
Query 19d >>             Runtime: 575 ms - Result correct: true
Query 20a >>             Runtime: 215 ms - Result correct: true
Query 20b >>             Runtime: 154 ms - Result correct: true
Query 20c >>             Runtime: 267 ms - Result correct: true
Query 21a >>             Runtime: 49 ms - Result correct: true
Query 21b >>             Runtime: 51 ms - Result correct: true
Query 21c >>             Runtime: 74 ms - Result correct: true
Query 22a >>             Runtime: 61 ms - Result correct: true
Query 22b >>             Runtime: 56 ms - Result correct: true
Query 22c >>             Runtime: 86 ms - Result correct: true
Query 22d >>             Runtime: 84 ms - Result correct: true
Query 23a >>             Runtime: 52 ms - Result correct: true
Query 23b >>             Runtime: 54 ms - Result correct: true
Query 23c >>             Runtime: 64 ms - Result correct: true
Query 24a >>             Runtime: 102 ms - Result correct: true
Query 24b >>             Runtime: 94 ms - Result correct: true
Query 25a >>             Runtime: 133 ms - Result correct: true
Query 25b >>             Runtime: 65 ms - Result correct: true
Query 25c >>             Runtime: 147 ms - Result correct: true
Query 26a >>             Runtime: 330 ms - Result correct: true
Query 26b >>             Runtime: 254 ms - Result correct: true
Query 26c >>             Runtime: 397 ms - Result correct: true
Query 27a >>             Runtime: 47 ms - Result correct: true
Query 27b >>             Runtime: 21 ms - Result correct: true
Query 27c >>             Runtime: 74 ms - Result correct: true
Query 28a >>             Runtime: 97 ms - Result correct: true
Query 28b >>             Runtime: 49 ms - Result correct: true
Query 28c >>             Runtime: 72 ms - Result correct: true
Query 29a >>             Runtime: 47 ms - Result correct: true
Query 29b >>             Runtime: 44 ms - Result correct: true
Query 29c >>             Runtime: 153 ms - Result correct: true
Query 30a >>             Runtime: 102 ms - Result correct: true
Query 30b >>             Runtime: 67 ms - Result correct: true
Query 30c >>             Runtime: 144 ms - Result correct: true
Query 31a >>             Runtime: 135 ms - Result correct: true
Query 31b >>             Runtime: 64 ms - Result correct: true
Query 31c >>             Runtime: 211 ms - Result correct: true
Query 32a >>             Runtime: 125 ms - Result correct: true
Query 32b >>             Runtime: 126 ms - Result correct: true
Query 33a >>             Runtime: 134 ms - Result correct: true
Query 33b >>             Runtime: 113 ms - Result correct: true
Query 33c >>             Runtime: 195 ms - Result correct: true
All queries succeeded: true
Total Runtime in ms: 18249 ms    
```

## Task

Given the joining pipeline and the pre-filtered input data, your task is to implement an efficient joining algorithm to accelerate the execution time of the joining pipeline. Specifically, you need to implement the following function in `src/execute.cpp`:

```C++
ColumnarTable execute(const Plan& plan, void* context);
```

Optionally, you can implement these two functions as well to prepare any global context (e.g., thread pool) to accelerate the execution.

```C++
void* build_context();
void destroy_context(void*);
```

### Input format

The input plan in the above function is defined as the following struct.

```C++
struct ScanNode {
    size_t base_table_id;
};

struct JoinNode {
    bool   build_left;
    size_t left;
    size_t right;
    size_t left_attr;
    size_t right_attr;
};

struct PlanNode {
    std::variant<ScanNode, JoinNode>          data;
    std::vector<std::tuple<size_t, DataType>> output_attrs;
};

struct Plan {
    std::vector<PlanNode>      nodes;
    std::vector<ColumnarTable> inputs;
    size_t root;
}
```

**Scan**:
- The `base_table_id` member refers to which input table in the `inputs` member of a plan is used by the Scan node.
- Each item in the `output_attrs` indicates which column in the base table should be output and what type it is.

**Join**:
- The `build_left` member refers to which side the hash table should be built on, where `true` indicates building the hash table on the left child, and `false` indicates the opposite.
- The `left` and `right` members are the indexes of the left and right child of the Join node in the `nodes` member of a plan, respectively.
- The `left_attr` and `right_attr` members are the join condition of Join node. Supposing that there are two records, `left_record` and `right_record`, from the intermediate results of the left and right child, respectively. The members indicate that the two records should be joined when `left_record[left_attr] == right_record[right_attr]`.
- Each item in the `output_attrs` indicates which column in the result of children should be output and what type it is. Supposing that the left child has $n_l$ columns and the right child has $n_r$ columns, the value of the index $i \in \{0, \dots, n_l + n_r - 1\}$, where the ranges $\{0, \dots, n_l - 1\}$ and $\{n_l, \dots, n_l + n_r - 1\}$ indicate the output column is from left and right child respectively.

**Root**: The `root` member of a plan indicates which node is the root node of the execution plan tree.

### Data format

The input and output data both follow a simple columnar data format.

```C++
enum class DataType {
    INT32,       // 4-byte integer
    INT64,       // 8-byte integer
    FP64,        // 8-byte floating point
    VARCHAR,     // string of arbitary length
};

constexpr size_t PAGE_SIZE = 8192;

struct alignas(8) Page {
    std::byte data[PAGE_SIZE];
};

struct Column {
    DataType           type;
    std::vector<Page*> pages;
};

struct ColumnarTable {
    size_t              num_rows;
    std::vector<Column> columns;
};
```

A `ColumnarTable` first stores how many rows the table has in the `num_rows` member, then stores each column seperately as a `Column`. Each `Column` has a type and stores the items of the column into several pages. Each page is of 8192 bytes. In each page:

- The first 2 bytes are a `uint16_t` which is the number of rows $n_r$ in the page.
- The following 2 bytes are a `uint16_t` which is the number of non-`NULL` values $n_v$ in the page.
- The first $n_r$ bits in the last $\left\lfloor\frac{(n_r + 7)}{8}\right\rfloor$ bytes is a bitmap indicating whether the corresponding row has value or is `NULL`.

**Fixed-length attribute**: There are $n_v$ contiguous values begins at the first aligned position. For example, in a `Page` of `INT32`, the first value is at `data + 4`. While in a `Page` of `INT64` and `FP64`, the first value is at `data + 8`.

**Variable-length attribute**: There are $n_v$ contigous offsets (`uint16_t`) begins at `data + 4` in a `Page`, followed by the content of the varchars which begins at `char_begin = data + 4 + n_r * 2`. Each offset indicates the ending offset of the corresponding `VARCHAR` with respect to the `char_begin`.

**Long string**: When the length of a string is longer than `PAGE_SIZE - 7`, it can not fit in a normal page. Special pages will be used to store such string. If $n_r$ `== 0xffff` or $n_r$ `== 0xfffe`, the `Page` is a special page for long string. `0xffff` means the page is the first page of a long string and `0xfffe` means the page is the following page of a long string. The following 2 bytes is a `uint16_t` indicating the number of chars in the page, beginning at `data + 4`.

## Requirement

- You can only modify the file `src/execute.cpp` in the project.
- You must not use any third-party libraries. If you are using libraries for development (e.g., for logging), ensure to remove them before the final submission.
- The joining pipeline (including order and build side) is optimized by PostgreSQL for `Hash Join` only. However, in the `execute` function, you are free to use other algorithms and change the pipeline, as long as the result is equivalent.
- For any struct listed above, all of there members are public. You can manipulate them in free functions as desired as long as the original files are not changed and the manipulated objects can be destructed properly.
- Your program will be evaluated on an unpublished benchmark sampled from the original JOB benchmark. You will not be able to access the test benchmark.

## Quick start

> [!TIP]
> Run all the following commands in the root directory of this project.

First, download the imdb dataset.

```bash
./download_imdb.sh
```

Second, build the project.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev
cmake --build build -- -j $(nproc)
```

Third, prepare the DuckDB database for correctness checking.

```bash
./build/build_database imdb.db
```

Now, you can run the tests:
```bash
./build/run plans.json
```
> [!TIP]
> If you want to use `Ninja Multi-Config` as the generator. The commands will look like:
>
>```bash
> cmake -S . -B build -Wno-dev -G "Ninja Multi-Config"
> cmake --build build --config Release -- -j $(nproc)
> ./build/Release/build_database imdb.db
> ./build/Release/run plans.json
> ```

# Hardware

The evaluation is automatically executed on four different servers. On multi-socket machines, the benchmarks are bound to a single socket (using `numactl -m 0 -N 0`).

 * **Intel #1**
    * CPU: 4x Intel Xeon E7-4880 v2 (SMT 2, 15 cores, 30 threads)
    * Main memory: 512 GB
 * **AMD #1**
    * CPU: 2x AMD EPYC 7F72 (SMT 2, 24 cores, 48 threads)
    * Main memory: 256 GB
 * **IBM #1**
    * CPU: 8x IBM Power8 (SMT 8, 12 cores, 96 threads)
    * Main memory: 1024 GB
 * **ARM #1**
    * CPU: 1x Ampere Altra Max (SMT 1, 128 cores, 128 threads)
    * Main memory: 512 GB


For the final evaluation after the submission deadline, four additional servers will be included. These additional servers cover the same platforms but might differ in the supported feature set as they can be significantly older or newer than the initial servers.
All servers run Ubuntu Linux with versions ranging from 20.04 to 24.04. Code is compiled with Clang 18.
