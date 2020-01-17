//
// Created by eulerdb on 2019-09-23.
//

#ifndef EULERDB_LOG_UTIL_H
#define EULERDB_LOG_UTIL_H

#include "spdlog/spdlog.h"
#include "spdlog/sinks/stdout_color_sinks.h"

namespace spd = spdlog;

#define LOG_ERROR_WRITE(TARGET, FORMAT,...) do{\
    spd::get(TARGET)->error("[{}:{}] " FORMAT, __FUNCTION__, __LINE__, ## __VA_ARGS__);}while(false);

#define LOG_WARNING_WRITE(TARGET, FORMAT,...) do{\
    spd::get(TARGET)->warn("[{}:{}] " FORMAT, __FUNCTION__, __LINE__, ## __VA_ARGS__);}while(false);

#define LOG_INFO_WRITE(TARGET, FORMAT,...) do{\
    spd::get(TARGET)->info("[{}:{}] " FORMAT, __FUNCTION__, __LINE__, ## __VA_ARGS__);}while(false);

#define LOG_DEBUG_WRITE(TARGET, FORMAT,...) do{\
    spd::get(TARGET)->debug("[{}:{}] " FORMAT, __FUNCTION__, __LINE__, ## __VA_ARGS__);}while(false);

static void initLog(const std::string& logName = "console", spd::level::level_enum logLevel = spd::level::trace) {
    auto console = spdlog::stdout_color_mt(logName);
    spd::get(logName)->set_level(logLevel);
}

#endif //EULERDB_LOG_UTIL_H
