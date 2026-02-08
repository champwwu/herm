#include <gtest/gtest.h>
#include <fstream>
#include <filesystem>
#include <nlohmann/json.hpp>
#include "config_manager.hpp"

using namespace herm::engine::common;

class ConfigManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create a temporary test config file
    test_config_path_ = std::filesystem::temp_directory_path() / "test_config.json";
    
    nlohmann::json config = {
      {"app", {
        {"log", {
          {"file", "test.log"},
          {"level", "debug"}
        }},
        {"rest_port", 8080}
      }},
      {"strategy", {
        {"rpc_port", "50051"},
        {"symbol", "BTCUSDT"}
      }},
      {"market_data", {
        {"binance", {
          {"spot", {
            {"websocket_url_template", "wss://test.com/ws/{symbol}"},
            {"rest_url", "https://api.test.com"}
          }}
        }}
      }},
      {"test_array", {1, 2, 3, 4, 5}},
      {"test_string_array", {"a", "b", "c"}},
      {"test_double_array", {1.1, 2.2, 3.3}},
      {"test_bool", true},
      {"test_int", 42},
      {"test_double", 3.14},
      {"test_string", "hello"}
    };
    
    std::ofstream file(test_config_path_);
    file << config.dump(2);
    file.close();
  }

  void TearDown() override {
    // Clean up test config file
    if (std::filesystem::exists(test_config_path_)) {
      std::filesystem::remove(test_config_path_);
    }
  }

  std::filesystem::path test_config_path_;
};

TEST_F(ConfigManagerTest, LoadFromFile_Success) {
  ConfigManager config;
  EXPECT_TRUE(config.LoadFromFile(test_config_path_.string()));
}

TEST_F(ConfigManagerTest, LoadFromFile_NonExistent) {
  ConfigManager config;
  EXPECT_FALSE(config.LoadFromFile("/nonexistent/file.json"));
}

TEST_F(ConfigManagerTest, GetString_ExistingKey) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  EXPECT_EQ(config.GetString("test_string"), "hello");
  EXPECT_EQ(config.GetString("app.log.file"), "test.log");
}

TEST_F(ConfigManagerTest, GetString_NonExistentKey) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  EXPECT_EQ(config.GetString("nonexistent"), "");
  EXPECT_EQ(config.GetString("nonexistent", "default"), "default");
}

TEST_F(ConfigManagerTest, GetInt_ExistingKey) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  EXPECT_EQ(config.GetInt("test_int"), 42);
  EXPECT_EQ(config.GetInt("app.rest_port"), 8080);
}

TEST_F(ConfigManagerTest, GetInt_NonExistentKey) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  EXPECT_EQ(config.GetInt("nonexistent"), 0);
  EXPECT_EQ(config.GetInt("nonexistent", 99), 99);
}

TEST_F(ConfigManagerTest, GetDouble_ExistingKey) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  EXPECT_DOUBLE_EQ(config.GetDouble("test_double"), 3.14);
}

TEST_F(ConfigManagerTest, GetDouble_NonExistentKey) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  EXPECT_DOUBLE_EQ(config.GetDouble("nonexistent"), 0.0);
  EXPECT_DOUBLE_EQ(config.GetDouble("nonexistent", 9.9), 9.9);
}

TEST_F(ConfigManagerTest, GetBool_ExistingKey) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  EXPECT_TRUE(config.GetBool("test_bool"));
}

TEST_F(ConfigManagerTest, GetBool_NonExistentKey) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  EXPECT_FALSE(config.GetBool("nonexistent"));
  EXPECT_TRUE(config.GetBool("nonexistent", true));
}

TEST_F(ConfigManagerTest, GetStringArray_ExistingKey) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  auto arr = config.GetStringArray("test_string_array");
  ASSERT_EQ(arr.size(), 3);
  EXPECT_EQ(arr[0], "a");
  EXPECT_EQ(arr[1], "b");
  EXPECT_EQ(arr[2], "c");
}

TEST_F(ConfigManagerTest, GetIntArray_ExistingKey) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  auto arr = config.GetIntArray("test_array");
  ASSERT_EQ(arr.size(), 5);
  EXPECT_EQ(arr[0], 1);
  EXPECT_EQ(arr[4], 5);
}

TEST_F(ConfigManagerTest, GetDoubleArray_ExistingKey) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  auto arr = config.GetDoubleArray("test_double_array");
  ASSERT_EQ(arr.size(), 3);
  EXPECT_DOUBLE_EQ(arr[0], 1.1);
  EXPECT_DOUBLE_EQ(arr[2], 3.3);
}

TEST_F(ConfigManagerTest, GetNodeArray_ExistingKey) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  auto node = config.GetNodeArray("test_array");
  EXPECT_TRUE(node.is_array());
  EXPECT_EQ(node.size(), 5);
}

TEST_F(ConfigManagerTest, GetNodeValue_ExistingKey) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  auto node = config.GetNodeValue("strategy");
  EXPECT_TRUE(node.is_object());
  EXPECT_TRUE(node.contains("rpc_port"));
}

TEST_F(ConfigManagerTest, SetString_Override) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  config.SetString("test_string", "overridden");
  EXPECT_EQ(config.GetString("test_string"), "overridden");
}

TEST_F(ConfigManagerTest, SetInt_Override) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  config.SetInt("test_int", 999);
  EXPECT_EQ(config.GetInt("test_int"), 999);
}

TEST_F(ConfigManagerTest, SetDouble_Override) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  config.SetDouble("test_double", 99.9);
  EXPECT_DOUBLE_EQ(config.GetDouble("test_double"), 99.9);
}

TEST_F(ConfigManagerTest, SetBool_Override) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  config.SetBool("test_bool", false);
  EXPECT_FALSE(config.GetBool("test_bool"));
}

TEST_F(ConfigManagerTest, GetConfigDir) {
  std::string dir = ConfigManager::GetConfigDir();
  EXPECT_FALSE(dir.empty());
}

TEST_F(ConfigManagerTest, PrintAllConfig) {
  ConfigManager config;
  ASSERT_TRUE(config.LoadFromFile(test_config_path_.string()));
  
  // Should not throw
  config.PrintAllConfig();
}
