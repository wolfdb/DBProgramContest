#pragma once

#include "Consts.hpp"
#include <fmt/core.h>

extern int32_t expected_query;
extern int32_t actually_query;
extern int32_t work_load;

#define log_print(format, ...) \
    do { if ( expected_query == actually_query && work_load == 312131 ) { fmt::print(stderr, format, __VA_ARGS__); } } while (0)
