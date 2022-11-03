#pragma once

#include "Consts.hpp"
#include <fmt/core.h>

#if PRINT_LOG
#define log_print(format, ...) \
    do { fmt::print(stderr, format, __VA_ARGS__); } while (0)
#else
#define log_print(format, ...) {}
#endif