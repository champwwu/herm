#include "instrument_registry.hpp"
#include <fstream>
#include <sstream>
#include <algorithm>
#include <spdlog/spdlog.h>

namespace herm {
namespace engine {
namespace common {

InstrumentRegistry& InstrumentRegistry::GetInstance() {
  static InstrumentRegistry instance;
  return instance;
}

bool InstrumentRegistry::LoadFromCSV(const std::string& csv_path) {
  std::ifstream file(csv_path);
  if (!file.is_open()) {
    SPDLOG_ERROR("Failed to open instruments CSV file: {}", csv_path);
    return false;
  }

  instruments_by_id_.clear();
  instruments_by_symbol_.clear();

  std::string line;
  bool is_first_line = true;

  while (std::getline(file, line)) {
    // Skip empty lines
    if (line.empty() || (line.find_first_not_of(" \t\r\n") == std::string::npos)) {
      continue;
    }

    // Skip header line
    if (is_first_line) {
      is_first_line = false;
      // Verify header format
      if (line.find("instrument_id") == std::string::npos) {
        SPDLOG_WARN("CSV header may be missing or incorrect, continuing anyway");
      }
      continue;
    }

    // Parse CSV line
    std::istringstream iss(line);
    std::string token;
    std::vector<std::string> tokens;

    while (std::getline(iss, token, ',')) {
      // Trim whitespace
      token.erase(0, token.find_first_not_of(" \t\r\n"));
      token.erase(token.find_last_not_of(" \t\r\n") + 1);
      tokens.push_back(token);
    }

    // Expected format: instrument_id,base_ccy,quote_ccy,instrument_type,exchange,lotsize,ticksize,min_order_size,max_order_size
    if (tokens.size() < 9) {
      SPDLOG_WARN("Invalid CSV line (insufficient columns): {}", line);
      continue;
    }

    auto instrument = std::make_unique<Instrument>();
    instrument->instrument_id = tokens[0];
    instrument->base_ccy = tokens[1];
    instrument->quote_ccy = tokens[2];

    // Parse instrument_type
    std::string type_str = tokens[3];
    std::transform(type_str.begin(), type_str.end(), type_str.begin(), ::toupper);
    if (type_str == "SPOT") {
      instrument->instrument_type = InstrumentType::SPOT;
    } else if (type_str == "PERP") {
      instrument->instrument_type = InstrumentType::PERP;
    } else {
      SPDLOG_WARN("Invalid instrument_type '{}' for instrument {}, skipping", type_str, instrument->instrument_id);
      continue;
    }

    instrument->exchange = tokens[4];

    // Parse numeric fields
    try {
      instrument->lotsize = std::stod(tokens[5]);
      instrument->ticksize = std::stod(tokens[6]);
      instrument->min_order_size = std::stod(tokens[7]);
      instrument->max_order_size = std::stod(tokens[8]);
    } catch (const std::exception& e) {
      SPDLOG_WARN("Failed to parse numeric fields for instrument {}: {}", instrument->instrument_id, e.what());
      continue;
    }

    // Store instrument
    std::string symbol = instrument->GetSymbol();
    const Instrument* instrument_ptr = instrument.get();
    instruments_by_id_[instrument->instrument_id] = std::move(instrument);
    instruments_by_symbol_[symbol].push_back(instrument_ptr);

    SPDLOG_DEBUG("Loaded instrument: {} ({})", instrument_ptr->instrument_id, symbol);
  }

  loaded_ = true;
  SPDLOG_INFO("Loaded {} instruments from {}", instruments_by_id_.size(), csv_path);
  return true;
}

const Instrument* InstrumentRegistry::GetInstrument(const std::string& instrument_id) const {
  auto it = instruments_by_id_.find(instrument_id);
  if (it != instruments_by_id_.end()) {
    return it->second.get();
  }
  return nullptr;
}

std::vector<const Instrument*> InstrumentRegistry::GetInstrumentsBySymbol(const std::string& symbol) const {
  auto it = instruments_by_symbol_.find(symbol);
  if (it != instruments_by_symbol_.end()) {
    return it->second;
  }
  return {};
}

std::vector<const Instrument*> InstrumentRegistry::GetAllInstruments() const {
  std::vector<const Instrument*> result;
  result.reserve(instruments_by_id_.size());
  for (const auto& [id, instrument] : instruments_by_id_) {
    result.push_back(instrument.get());
  }
  return result;
}

}  // namespace common
}  // namespace engine
}  // namespace herm
