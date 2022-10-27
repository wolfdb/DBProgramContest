#pragma once

#include<thread>

#define DBCONTEST_PAGE_SIZE 4096
#define DBCONTEST_PAGE_ITEM_COUNT (DBCONTEST_PAGE_SIZE >> 3)

#define DBCONTEST_PAGE_ALIGN(LEN)   \
    (((uint64_t) (LEN) + ((DBCONTEST_PAGE_SIZE) - 1)) & ~((uint64_t) ((DBCONTEST_PAGE_SIZE) - 1)))

#define DBCONTEST_ITEM_COUNT_ALIGN(CNT)   \
    (((uint64_t) (CNT) + ((DBCONTEST_PAGE_ITEM_COUNT) - 1)) & ~((uint64_t) ((DBCONTEST_PAGE_ITEM_COUNT) - 1)))

// sample max 1MB for each column
#define MAX_SAMPLE_SIZE 1048576UL

// minimal scan size 4MB
#define MIN_SCAN_SIZE 4194304UL

#define MAX_SAMPLE_ITEM_COUNT (MAX_SAMPLE_SIZE >> 3)

#define MIN_SCAN_ITEM_COUNT (MIN_SCAN_SIZE >> 3)

#define HISTOGRAM_BUCKET_CNT 32