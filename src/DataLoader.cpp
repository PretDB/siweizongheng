#include "DataLoader.h"
#include "Row.h"

#include <boost/algorithm/string/split.hpp>
#include <boost/lexical_cast.hpp>
#include <fstream>
#include <memory>
#include <string>
#include <vector>


Rows LoadData(const std::string& path) {
    std::ifstream ifs(path);
    if (ifs.is_open() == false) {
        throw std::string("bad file name");
    }

    std::string meta_record_len_raw;
    std::getline(ifs, meta_record_len_raw);
    int32_t meta_records_len = boost::lexical_cast<int32_t>(meta_record_len_raw);

    Row* buf = new Row[meta_records_len];
    Rows ret = { buf, meta_records_len };

    for (int32_t r = 0; r < meta_records_len ; ++r) {
        std::string line;
        std::getline(ifs, line);
        std::vector<std::string> fields;
        boost::split(fields, line, [](char c) { return c == ','; });
        Row row = { boost::lexical_cast<int32_t>(fields[0]), boost::lexical_cast<int32_t>(fields[1]) };
        buf[r] = row;
    }

    return ret;
}
