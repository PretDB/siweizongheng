#include "DataGenerator.h"
#include "Row.h"

#include "glog/logging.h"

#include <random>

std::default_random_engine dre;
std::vector<int> a_candidates { 1000, 2000, 3000, 500, 1100, 6000 };


Rows GenerateData(int32_t count) {
    std::uniform_int_distribution<> di_a(0, 5);
    std::uniform_int_distribution<> di_b(0, 200);

    void* data = malloc(sizeof(Row) * count);
    LOG_IF(FATAL, data == nullptr) << "FAILED to allocate space for generated data";

    Rows rows;
    rows.data = reinterpret_cast<Row*>(data);
    rows.len = count;

    for (int i = 0; i < rows.len; ++i) {
        int a_index = di_a(dre);
        rows.data[i].a = a_candidates[a_index];
        rows.data[i].b = di_b(dre);
    }

    return rows;
}   // Rows GenerateData(int32_t count)
