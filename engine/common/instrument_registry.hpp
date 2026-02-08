#pragma once

#include <string>
#include <vector>
#include <unordered_map>
#include <memory>

namespace herm {
namespace engine {
namespace common {

// Instrument type enumeration
enum class InstrumentType {
  SPOT,
  PERP
};

// Instrument structure containing all static data
struct Instrument {
  std::string instrument_id;      // Unique identifier like "BTCUSDT.SPOT.BNC"
  std::string base_ccy;            // Base currency (e.g., "BTC")
  std::string quote_ccy;           // Quote currency (e.g., "USDT")
  InstrumentType instrument_type;  // SPOT or PERP
  std::string exchange;            // Exchange code (BNC, OKX, BYB, MOK, KRK)
  double lotsize;                  // Lot size
  double ticksize;                 // Tick size
  double min_order_size;           // Minimum order size
  double max_order_size;           // Maximum order size

  // Helper method to get symbol (base_ccy + quote_ccy)
  std::string GetSymbol() const {
    return base_ccy + quote_ccy;
  }
};

// Registry for managing instruments (acts as static database)
class InstrumentRegistry {
 public:
  // Get singleton instance
  static InstrumentRegistry& GetInstance();

  // Load instruments from CSV file
  // CSV format: instrument_id,base_ccy,quote_ccy,instrument_type,exchange,lotsize,ticksize,min_order_size,max_order_size
  // Returns true on success, false on failure
  bool LoadFromCSV(const std::string& csv_path);

  // Get instrument by ID
  // Returns nullptr if not found
  const Instrument* GetInstrument(const std::string& instrument_id) const;

  // Get all instruments for a given symbol (e.g., "BTCUSDT")
  std::vector<const Instrument*> GetInstrumentsBySymbol(const std::string& symbol) const;

  // Get all instruments
  std::vector<const Instrument*> GetAllInstruments() const;

  // Check if registry is loaded
  bool IsLoaded() const { return loaded_; }

 private:
  InstrumentRegistry() = default;
  ~InstrumentRegistry() = default;
  InstrumentRegistry(const InstrumentRegistry&) = delete;
  InstrumentRegistry& operator=(const InstrumentRegistry&) = delete;

  std::unordered_map<std::string, std::unique_ptr<Instrument>> instruments_by_id_;
  std::unordered_map<std::string, std::vector<const Instrument*>> instruments_by_symbol_;
  bool loaded_ = false;
};

}  // namespace common
}  // namespace engine
}  // namespace herm
