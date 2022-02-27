#include "Column.h"
#include "DataGenerator.h"
#include "DataLoader.h"
#include "Row.h"

#include <benchmark/benchmark.h>
#include <glog/logging.h>

#include <algorithm>
#include <boost/algorithm/string/split.hpp>
#include <boost/range/algorithm/binary_search.hpp>
#include <boost/lexical_cast.hpp>
#include <fcntl.h>
#include <future>
#include <list>
#include <memory>
#include <vector>
#include <thread>

typedef std::vector<std::list<Row>> ParallelRowsResult;

Rows rows;

void print_one(const Row& row) {
    // FLAGS_log_dir = "log";
    // LOG(WARNING) << row.a << '\t' << row.b;
}   // void print_one(const Row& row)

void raw_print(Row* row, int32_t len) {
    for (int32_t i = 0; i < len; ++i) {
        print_one(row[i]);
    }
}   // void raw_print(Row* row, int32_t len)

void sort_by_a_b(const Rows& rows) {
    std::sort(rows.data, rows.data + rows.len, [](const Row& a, const Row& b) {
            if (a.a < b.a) {
                return true;
            } else if (a.a == b.a && a.b < b.b) {
                return true;
            } else {
                return false;
            }
        });
}   // void sort_by_a_b(Row* rows, int32_t len)

bool task1_predicate(const Row& row) {
    return row.b >= 10 && row.b < 50 && (row.a == 1000 || row.a == 2000 || row.a == 3000);
}   // bool task1_predicate(const Row& row)


void task1(Row* row, int32_t len) {
    auto conditional_print = [] (const Row& row) {
        if (task1_predicate(row)) {
            print_one(row);
        }
    };
    std::for_each(row, row + len, conditional_print);
}   // void task1(Row* row, int32_t len)

Columns vectorize(Row* rows, int32_t len) {
    Columns cols;
    return cols;
}

void task2_bin_search(Row* rows, int32_t len) {
    std::list<Row> res;

    auto bound_searcher = [](const Row& a, const Row& b) {
        return a.a < b.a;
    };

    Row val_1000 {1000, 0};
    auto p_range_1000 = std::equal_range(
            rows,
            rows + len,
            val_1000,
            bound_searcher
            );
    if (p_range_1000.first != rows + len && p_range_1000.second != rows + len) {
        std::for_each(
                p_range_1000.first,
                p_range_1000.second,
                [&res](const Row& row) {
                    res.push_back(row);
                }
                );
    }

    Row val_2000 { 2000, 0 };
    auto p_range_2000 = std::equal_range(
            p_range_1000.second,
            rows + len,
            val_2000,
            bound_searcher
            );
    if (p_range_2000.first != rows + len && p_range_2000.second != rows + len) {
        std::for_each(
                p_range_2000.first,
                p_range_2000.second,
                [&res](const Row& row) {
                    res.push_back(row);
                }
                );
    }

    Row val_3000 { 3000, 0 };
    auto p_range_3000 = std::equal_range(
            p_range_2000.second,
            rows + len,
            val_3000,
            bound_searcher
            );
    if (p_range_3000.first != rows + len && p_range_3000.second != rows + len) {
        std::for_each(
                p_range_3000.first,
                p_range_3000.second,
                [&res](const Row& row) {
                    res.push_back(row);
                }
                );
    }
    std::for_each(
            res.begin(),
            res.end(),
            [](const Row& row) {
                if (row.b >= 10 && row.b < 50) {
                    print_one(row);
                }
            }
            );
}   // void task2_bin_search(Row* rows, int32_t len)

void task2_vectorize(Row* rows, int32_t len) {
    auto vectorize = [](const Row* rows, int32_t len) {
    };  // auto vectorize = [](const Row* rows, int32_t len)
}   // void task2_vectorize(Row* row, int32_t len)

void task2_vert(Row* rows, int32_t len) {
    auto vertical_parallel = [](const Row* rows, int32_t len) {
        auto pred1 = [](const Row& row) {
            return row.a == 1000 && row.b >= 10 && row.b < 50;
        };
        auto pred2 = [](const Row& row) {
            return row.a == 2000 && row.b >= 10 && row.b < 50;
        };
        auto pred3 = [](const Row& row) {
            return row.a == 3000 && row.b >= 10 && row.b < 50;
        };
        std::vector<bool> res1(len);
        std::vector<bool> res2(len);
        std::vector<bool> res3(len);

    };  // auto vertical_parallel = [](const Row* rows, int32_t len)
}   // void task2_vert(Row* row, int32_t len)

ParallelRowsResult hori_search(Row* rows, int32_t len) {
    auto hori_pred = [](const Row& row) {
        return (row.a == 1000 || row.a == 2000 || row.a == 3000) && row.b >= 10 && row.b < 50;
    };  // auto hori_pred = [](const Row& row)

    auto hori_task = [&hori_pred](const Row* rows, int32_t len) {
        std::list<Row> res;
        for (int32_t i = 0; i < len; ++i) {
            if (hori_pred(rows[i])) {
                res.push_back(rows[i]);
            }
        }
        return res;
    };

    std::future<std::list<Row>> part1(std::async(hori_task, rows, len / 2));
    std::future<std::list<Row>> part2(std::async(hori_task, rows + len / 2, len - len / 2));

    ParallelRowsResult res;
    res.push_back(part1.get());
    res.push_back(part2.get());

    return res;
}   // void hori_search(Row* rows, int32_t)

void task2_hori(Row* rows, int32_t len) {
    ParallelRowsResult gathered = hori_search(rows, len);

    for (auto& batch : gathered) {
        for(const Row& r: batch) {
            print_one(r);
        }
    }
}   // void task2_hori(Row* row, int32_t len)

void task2_create_index(Rows& rows) {
    std::unique_ptr<Indexes> index_a(new Indexes());
    std::unique_ptr<Indexes> index_b(new Indexes());

    for (int i = 0; i < rows.len; ++i) {
        index_a->insert({ rows.data[i].a, i });
        index_b->insert({ rows.data[i].b, i });
    }   // for (int i = 0; i < rows.len; ++i)

    rows.index_a = std::move(index_a);
    rows.index_b = std::move(index_b);
}   // void task2_create_index(Rows& rows)

void task2_indexed(const Rows& rows) {
}   // void task2_indexed(Rows* rows, int32_t len)

void task3_by_hori(Row* rows, int32_t len) {
    ParallelRowsResult res = hori_search(rows, len);

    int32_t res_total_size = 0;
    for (auto& batch : res) {
        res_total_size += batch.size();
    }
    std::vector<Row> merged_res(res_total_size);

    int32_t i = 0;
    for (auto& batch : res) {
        for (auto& row : batch) {
            merged_res[i] = row;
            ++i;
        }
    }

    std::sort(merged_res.begin(),
            merged_res.end(),
            [] (const Row& a, const Row& b) {
                return a.b < b.b;
            }
            );

    std::vector<Row>& sorted_res = merged_res;
    for (const Row& row : sorted_res) {
        print_one(row);
    }
}   // void task3_by_hori(Row* rows, int32_t len)

static void BM_SetUp(const ::benchmark::State& state) {
    rows = GenerateData(state.range(0));
}   // static void BM_SetUp(const ::benchmark::State& state)

static void BM_TearDown(const ::benchmark::State& state) {
    delete[] rows.data;
}   // static void BM_TearDown(const ::benchmark::State& state)

static void BM_task1(::benchmark::State& state) {
    for (auto _ : state) {
        task1(rows.data, rows.len);
    }
}   // static void BM_task1(::benchmark::State& state)

static void BM_task2_setup(const ::benchmark::State& state) {
    rows = GenerateData(state.range(0));
    sort_by_a_b(rows);
}
static void BM_task2_hori(::benchmark::State& state) {
    for (auto _ : state) {
        task2_hori(rows.data, rows.len);
    }
}   // static void BM_task2_hori(::benchmark::State& state)

static void BM_task2_bin_search(::benchmark::State& state) {
    for (auto _ : state) {
        task2_bin_search(rows.data, rows.len);
    }
}   // static void BM_task2_bin_search(::benchmark::State& state)

static void BM_task3_by_hori(::benchmark::State& state) {
    for (auto _ : state) {
        task3_by_hori(rows.data, rows.len);
    }
}   // static void BM_task3_by_hori(::benchmark::State& state)

BENCHMARK(BM_task1)->Range(8, 8 << 15)->Setup(BM_SetUp)->Teardown(BM_TearDown);
BENCHMARK(BM_task2_hori)->Range(8, 8 << 15)->Setup(BM_task2_setup)->Teardown(BM_TearDown);
BENCHMARK(BM_task2_bin_search)->Range(8, 8 << 15)->Setup(BM_task2_setup)->Teardown(BM_TearDown);
BENCHMARK(BM_task3_by_hori)->Range(8, 8 << 15)->Setup(BM_task2_setup)->Teardown(BM_TearDown);
BENCHMARK_MAIN();
