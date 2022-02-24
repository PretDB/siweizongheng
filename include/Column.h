#ifndef COLUMN_H
#define COLUMN_H

#include <cstdint>

struct Column {
    int32_t*    data;
    int32_t     len;
};  // struct Column

struct Columns {
    Column*     data;
    int32_t     len;
};  // struct Columns
#endif  // COLUMN_H
