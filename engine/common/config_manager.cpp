#include "config_manager.hpp"
#include <spdlog/spdlog.h>
#include <fstream>

namespace herm {
namespace engine {
namespace common {

ConfigManager::ConfigManager() {
}

bool ConfigManager::LoadFromFile(const std::string& config_path) {
  try {
    std::ifstream file(config_path);
    if (!file.is_open()) {
      SPDLOG_WARN("Failed to open config file: {}", config_path);
      return false;
    }
    
    file >> config_root_;
    
    if (config_root_.is_object()) {
      SPDLOG_INFO("Loaded config from: {} (has {} top-level keys)", config_path, config_root_.size());
      // Log top-level keys for debugging
      std::string keys;
      for (auto it = config_root_.begin(); it != config_root_.end(); ++it) {
        if (!keys.empty()) keys += ", ";
        keys += it.key();
      }
      SPDLOG_TRACE("Config top-level keys: {}", keys);
    } else {
      SPDLOG_WARN("Config file loaded but root is not an object");
      return false;
    }
    return true;
  } catch (const std::exception& e) {
    SPDLOG_WARN("Failed to load config from {}: {}", config_path, e.what());
    return false;
  }
}

void ConfigManager::PrintAllConfig() const {
  SPDLOG_INFO("Printing all config");
  try {
    // Pretty-print the loaded JSON config to the log
    std::string pretty_json = config_root_.dump(2);
    SPDLOG_INFO("\n{}", pretty_json);
  } catch (const std::exception& e) {
    SPDLOG_ERROR("Failed to print all config: {}", e.what());
  }
}


std::string ConfigManager::GetConfigDir() {
  const char* env = std::getenv("herm_CONFIG_DIR");
  return env ? std::string(env) : "config";
}

std::string ConfigManager::GetString(const std::string& key, const std::string& default_value) const {
  // Check overrides first
  auto it = overrides_.find(key);
  if (it != overrides_.end()) {
    SPDLOG_TRACE("GetString('{}'): found in overrides: {}", key, it->second);
    return it->second;
  }
  
  // Check JSON config
  nlohmann::json node = GetNode(key);
  if (node.is_string()) {
    std::string value = node.get<std::string>();
    SPDLOG_TRACE("GetString('{}'): found in config: {}", key, value);
    return value;
  }
  
  SPDLOG_TRACE("GetString('{}'): not found, using default: {}", key, default_value);
  return default_value;
}

int ConfigManager::GetInt(const std::string& key, int default_value) const {
  auto it = overrides_.find(key);
  if (it != overrides_.end()) {
    try {
      int value = std::stoi(it->second);
      SPDLOG_TRACE("GetInt('{}'): found in overrides: {}", key, value);
      return value;
    } catch (...) {
      return default_value;
    }
  }
  
  nlohmann::json node = GetNode(key);
  if (node.is_null() || !node.is_number()) {
    SPDLOG_TRACE("GetInt('{}'): node not a number, using default {}", key, default_value);
    return default_value;
  }
  
  try {
    int value = node.get<int>();
    SPDLOG_TRACE("GetInt('{}'): found value {}", key, value);
    return value;
  } catch (const std::exception& e) {
    SPDLOG_TRACE("GetInt('{}'): conversion error: {}, using default {}", key, e.what(), default_value);
    return default_value;
  }
}

double ConfigManager::GetDouble(const std::string& key, double default_value) const {
  auto it = overrides_.find(key);
  if (it != overrides_.end()) {
    try {
      return std::stod(it->second);
    } catch (...) {
      return default_value;
    }
  }
  
  nlohmann::json node = GetNode(key);
  if (node.is_null() || !node.is_number()) {
    return default_value;
  }
  
  try {
    return node.get<double>();
  } catch (...) {
    return default_value;
  }
}

bool ConfigManager::GetBool(const std::string& key, bool default_value) const {
  auto it = overrides_.find(key);
  if (it != overrides_.end()) {
    std::string val = it->second;
    if (val == "true" || val == "1" || val == "yes") return true;
    if (val == "false" || val == "0" || val == "no") return false;
    return default_value;
  }
  
  nlohmann::json node = GetNode(key);
  if (node.is_null() || !node.is_boolean()) {
    return default_value;
  }
  
  try {
    return node.get<bool>();
  } catch (...) {
    return default_value;
  }
}

nlohmann::json ConfigManager::GetNodeArray(const std::string& key) const {
  nlohmann::json node = GetNode(key);
  if (node.is_array()) {
    return node;
  }
  SPDLOG_DEBUG("GetNodeArray('{}'): node is not an array. is_null: {}, is_array: {}", 
                key, node.is_null(), node.is_array());
  return nlohmann::json();
}

nlohmann::json ConfigManager::GetNodeValue(const std::string& key) const {
  return GetNode(key);
}

std::vector<std::string> ConfigManager::GetStringArray(const std::string& key) const {
  std::vector<std::string> result;
  nlohmann::json node = GetNodeArray(key);
  if (node.is_array()) {
    for (const auto& item : node) {
      try {
        if (item.is_string()) {
          result.push_back(item.get<std::string>());
        }
      } catch (...) {
        // Skip invalid items
      }
    }
  }
  return result;
}

std::vector<int> ConfigManager::GetIntArray(const std::string& key) const {
  std::vector<int> result;
  nlohmann::json node = GetNodeArray(key);
  if (node.is_array()) {
    for (const auto& item : node) {
      try {
        if (item.is_number_integer()) {
          result.push_back(item.get<int>());
        }
      } catch (...) {
        // Skip invalid items
      }
    }
  }
  return result;
}

std::vector<double> ConfigManager::GetDoubleArray(const std::string& key) const {
  std::vector<double> result;
  nlohmann::json node = GetNodeArray(key);
  if (node.is_array()) {
    for (const auto& item : node) {
      try {
        if (item.is_number()) {
          result.push_back(item.get<double>());
        }
      } catch (...) {
        // Skip invalid items
      }
    }
  }
  return result;
}

void ConfigManager::SetString(const std::string& key, const std::string& value) {
  overrides_[key] = value;
}

void ConfigManager::SetInt(const std::string& key, int value) {
  overrides_[key] = std::to_string(value);
}

void ConfigManager::SetDouble(const std::string& key, double value) {
  overrides_[key] = std::to_string(value);
}

void ConfigManager::SetBool(const std::string& key, bool value) {
  overrides_[key] = value ? "true" : "false";
}

nlohmann::json ConfigManager::GetNode(const std::string& key) const {
  // Support dot notation: "server.port"
  std::vector<std::string> parts;
  std::string current;
  for (char c : key) {
    if (c == '.') {
      if (!current.empty()) {
        parts.push_back(current);
        current.clear();
      }
    } else {
      current += c;
    }
  }
  if (!current.empty()) {
    parts.push_back(current);
  }
  
  nlohmann::json node = config_root_;
  if (node.is_null()) {
    SPDLOG_DEBUG("GetNode('{}'): config_root_ is null", key);
    return nlohmann::json();  // Config not loaded
  }
  
  if (parts.empty()) {
    return node;  // Empty key means root
  }
  
  for (const auto& part : parts) {
    if (!node.is_object()) {
      SPDLOG_DEBUG("GetNode('{}'): node is not an object at part '{}'", key, part);
      return nlohmann::json();
    }
    if (!node.contains(part)) {
      SPDLOG_DEBUG("GetNode('{}'): key '{}' not found", key, part);
      return nlohmann::json();
    }
    node = node[part];
  }
  
  return node;
}

}  // namespace common
}  // namespace engine
}  // namespace herm
