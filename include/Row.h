#ifndef ROW_H
#define ROW_H

#include <cstdint>
#include <memory>

struct Row {
    int32_t a;
    int32_t b;
};  // struct Row

struct Rows {
    Row*    data;
    int32_t len;
};  // struct Rows

#endif  // ROW_H
