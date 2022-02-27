#ifndef ROW_H
#define ROW_H

#include <cstdint>
#include <memory>
#include <map>

typedef std::multimap<int32_t, int32_t> Indexes ;

struct Row {
    int32_t a;
    int32_t b;
};  // struct Row

struct Rows {
    Row*                        data;
    int32_t                     len;
    std::unique_ptr<Indexes>    index_a;
    std::unique_ptr<Indexes>    index_b;
};  // struct Rows

#endif  // ROW_H
