#pragma once

#include <absl/base/log_severity.h>
#include <absl/log/log_entry.h>
#include <absl/log/log_sink.h>
#include <absl/log/log_sink_registry.h>
#include <absl/log/globals.h>
#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <nlohmann/json.hpp>
#include <fstream>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#if defined(_WIN32) || defined(_WIN64)
#include <io.h>
#include <fcntl.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace herm {
namespace engine {
namespace common {

// Unified configuration manager
class ConfigManager {
 public:
  ConfigManager();
  ~ConfigManager() = default;

  // Load configuration from JSON file
  bool LoadFromFile(const std::string& config_path);

  // Print all config
  void PrintAllConfig() const;
  
  // Get config directory
  static std::string GetConfigDir();

  // Get string value
  std::string GetString(const std::string& key, const std::string& default_value = "") const;

  // Get int value
  int GetInt(const std::string& key, int default_value = 0) const;

  // Get double value
  double GetDouble(const std::string& key, double default_value = 0.0) const;

  // Get bool value
  bool GetBool(const std::string& key, bool default_value = false) const;

  // Get array node (JSON array)
  nlohmann::json GetNodeArray(const std::string& key) const;
  
  // Get any node (object, array, string, number, etc.)
  nlohmann::json GetNodeValue(const std::string& key) const;

  // Get string array
  std::vector<std::string> GetStringArray(const std::string& key) const;

  // Get int array
  std::vector<int> GetIntArray(const std::string& key) const;

  // Get double array
  std::vector<double> GetDoubleArray(const std::string& key) const;

  // Set value (for command-line overrides)
  void SetString(const std::string& key, const std::string& value);
  void SetInt(const std::string& key, int value);
  void SetDouble(const std::string& key, double value);
  void SetBool(const std::string& key, bool value);

 private:
  nlohmann::json config_root_;
  std::unordered_map<std::string, std::string> overrides_;
  
  nlohmann::json GetNode(const std::string& key) const;
};

// Legacy standalone function (for backward compatibility)
inline std::string GetConfigDir() {
  return ConfigManager::GetConfigDir();
}

}  // namespace common
}  // namespace engine
}  // namespace herm
