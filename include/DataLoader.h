#ifndef DATALOADER_H
#define DATALOADER_H

#include "Row.h"

#include <memory>
#include <vector>

Rows LoadData(const std::string& path);
#endif  // DATALOADER_H
