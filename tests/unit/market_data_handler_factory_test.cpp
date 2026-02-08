#include <gtest/gtest.h>
#include "market_data_handler_factory.hpp"
#include "market_data_handler.hpp"
#include "config_manager.hpp"

using namespace herm;
using namespace herm::engine::common;

// Mock handler for testing
class MockTestHandler : public MarketDataHandler {
 public:
  MockTestHandler() : MarketDataHandler("MOCK_TEST") {}
  
  void Initialize(const ConfigManager& config) override {
    initialized_ = true;
  }
  
  bool Subscribe(const Instrument& instrument) override {
    subscribed_ = true;
    return true;
  }
  
  bool Unsubscribe(const std::string& instrument_id) override {
    subscribed_ = false;
    return true;
  }
  
  bool Connect() override { return true; }
  void Disconnect() override {}
  bool IsConnected() const override { return true; }
  
  bool ParseMessage(const std::string& message,
                    std::vector<PriceLevel>& bids,
                    std::vector<PriceLevel>& asks) override {
    return false;  // Mock implementation
  }
  
  bool initialized_ = false;
  bool subscribed_ = false;
};

class MarketDataHandlerFactoryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    factory_ = &MarketDataHandlerFactory::GetInstance();
    config_ = std::make_unique<ConfigManager>();
  }

  MarketDataHandlerFactory* factory_;
  std::unique_ptr<ConfigManager> config_;
};

TEST_F(MarketDataHandlerFactoryTest, GetInstance_ReturnsSingleton) {
  auto& instance1 = MarketDataHandlerFactory::GetInstance();
  auto& instance2 = MarketDataHandlerFactory::GetInstance();
  
  EXPECT_EQ(&instance1, &instance2);
}

TEST_F(MarketDataHandlerFactoryTest, RegisterHandler_AndCreate) {
  // Register a custom handler
  factory_->RegisterHandler("test_handler", [](const ConfigManager& config) {
    return std::make_unique<MockTestHandler>();
  });
  
  EXPECT_TRUE(factory_->IsRegistered("test_handler"));
  
  auto handler = factory_->Create("test_handler", *config_);
  ASSERT_NE(handler, nullptr);
  EXPECT_EQ(handler->GetExchangeName(), "MOCK_TEST");
}

TEST_F(MarketDataHandlerFactoryTest, Create_NonExistentType) {
  auto handler = factory_->Create("nonexistent_type", *config_);
  EXPECT_EQ(handler, nullptr);
}

TEST_F(MarketDataHandlerFactoryTest, IsRegistered_BuiltInHandlers) {
  // These should be registered by static initialization
  EXPECT_TRUE(factory_->IsRegistered("binance_spot"));
  EXPECT_TRUE(factory_->IsRegistered("binance_futures"));
  EXPECT_TRUE(factory_->IsRegistered("bybit"));
  EXPECT_TRUE(factory_->IsRegistered("okx"));
  EXPECT_TRUE(factory_->IsRegistered("mock"));
  EXPECT_TRUE(factory_->IsRegistered("mok"));  // Alias
}

TEST_F(MarketDataHandlerFactoryTest, Create_BuiltInHandlers) {
  // Test creating built-in handlers (they may fail to initialize without proper config,
  // but factory should return non-null)
  auto binance_spot = factory_->Create("binance_spot", *config_);
  // Handler creation should succeed (initialization may fail later)
  EXPECT_NE(binance_spot, nullptr);
  
  auto mock_handler = factory_->Create("mock", *config_);
  EXPECT_NE(mock_handler, nullptr);
}

TEST_F(MarketDataHandlerFactoryTest, RegisterHandler_Overwrite) {
  bool first_called = false;
  bool second_called = false;
  
  // Register first handler
  factory_->RegisterHandler("overwrite_test", [&first_called](const ConfigManager& config) {
    first_called = true;
    return std::make_unique<MockTestHandler>();
  });
  
  // Register second handler (should overwrite)
  factory_->RegisterHandler("overwrite_test", [&second_called](const ConfigManager& config) {
    second_called = true;
    return std::make_unique<MockTestHandler>();
  });
  
  auto handler = factory_->Create("overwrite_test", *config_);
  ASSERT_NE(handler, nullptr);
  
  EXPECT_FALSE(first_called);
  EXPECT_TRUE(second_called);
}

TEST_F(MarketDataHandlerFactoryTest, Create_BinanceAlias) {
  // "binance" should create a BinanceSpotHandler
  auto handler = factory_->Create("binance", *config_);
  EXPECT_NE(handler, nullptr);
  // Should be a BinanceSpotHandler (we can't easily check type, but it shouldn't be null)
}

TEST_F(MarketDataHandlerFactoryTest, MultipleRegistrations) {
  int call_count = 0;
  
  factory_->RegisterHandler("multi_test", [&call_count](const ConfigManager& config) {
    call_count++;
    return std::make_unique<MockTestHandler>();
  });
  
  // Create multiple handlers
  auto h1 = factory_->Create("multi_test", *config_);
  auto h2 = factory_->Create("multi_test", *config_);
  auto h3 = factory_->Create("multi_test", *config_);
  
  EXPECT_NE(h1, nullptr);
  EXPECT_NE(h2, nullptr);
  EXPECT_NE(h3, nullptr);
  EXPECT_EQ(call_count, 3);
}
