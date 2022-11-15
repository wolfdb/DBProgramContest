#pragma once

#include "Consts.hpp"
#include <fmt/core.h>

extern int32_t expected_query;
extern int32_t actually_query;
extern int32_t work_load;

// small workload: 1561

#define log_print(format, ...) \
    do { if ( expected_query == actually_query && work_load == 24328 ) { fmt::print(stderr, format, __VA_ARGS__); } } while (0)