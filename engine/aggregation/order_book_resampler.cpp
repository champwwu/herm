#include "order_book_resampler.hpp"
#include <spdlog/spdlog.h>
#include <chrono>
#include <map>
#include <cmath>
#include <algorithm>

namespace herm {
namespace market_data {

void OrderBookResampler::ResampleOrderBook(
    const ::herm::market_data::OrderBook& full_book,
    RequestType request_type,
    const std::vector<int32_t>& basis_points,
    const std::vector<double>& notional_volumes,
    ::herm::market_data::OrderBook* output) {
  
  if (request_type == RequestType::NONE) {
    output->CopyFrom(full_book);
    return;
  }
  
  // Initialize output with basic fields
  output->set_symbol(full_book.symbol());
  auto now = std::chrono::duration_cast<std::chrono::microseconds>(
      std::chrono::system_clock::now().time_since_epoch()).count();
  output->set_timestamp_us(now);
  
  if (request_type == RequestType::BBO) {
    ApplyBBO(full_book, output);
  } else if (request_type == RequestType::PRICE_BAND) {
    ApplyPriceBand(full_book, basis_points, output);
  } else if (request_type == RequestType::VOLUME_BAND) {
    ApplyVolumeBand(full_book, notional_volumes, output);
  }
}

void OrderBookResampler::ApplyBBO(
    const ::herm::market_data::OrderBook& full_book,
    ::herm::market_data::OrderBook* output) {
  
  if (full_book.bids_size() > 0) {
    output->add_bids()->CopyFrom(full_book.bids(0));
  }
  if (full_book.asks_size() > 0) {
    output->add_asks()->CopyFrom(full_book.asks(0));
  }
  *output->mutable_venue_timestamps() = full_book.venue_timestamps();
}

void OrderBookResampler::ApplyPriceBand(
    const ::herm::market_data::OrderBook& full_book,
    const std::vector<int32_t>& basis_points,
    ::herm::market_data::OrderBook* output) {
  
  double bbo_bid = 0.0, bbo_ask = 0.0;
  bool has_bid = false, has_ask = false;
  if (full_book.bids_size() > 0) {
    bbo_bid = full_book.bids(0).price();
    has_bid = true;
  }
  if (full_book.asks_size() > 0) {
    bbo_ask = full_book.asks(0).price();
    has_ask = true;
  }
  
  if (!has_bid && !has_ask) {
    return;
  }
  
  // Sort basis points in ascending order for accumulation
  auto sorted_bps = basis_points;
  std::sort(sorted_bps.begin(), sorted_bps.end());
  
  // Helper lambda to process price bands (bids or asks)
  auto process_price_band = [&](bool is_bid) {
    double bbo_price = is_bid ? bbo_bid : bbo_ask;
    int size = is_bid ? full_book.bids_size() : full_book.asks_size();
    
    double accumulated_quantity = 0.0;
    std::map<std::string, double> venue_quantities;
    size_t idx = 0;
    
    for (int32_t bps : sorted_bps) {
      double target_price = is_bid ? bbo_price * (1.0 - bps / 10000.0) 
                                    : bbo_price * (1.0 + bps / 10000.0);
      
      // Accumulate all levels from current position to target price
      while (idx < static_cast<size_t>(size)) {
        const auto& level = is_bid ? full_book.bids(idx) : full_book.asks(idx);
        
        // Check if we've reached the target
        bool should_stop = is_bid ? (level.price() < target_price) 
                                   : (level.price() > target_price);
        if (should_stop) {
          break;
        }
        
        accumulated_quantity += level.quantity();
        for (const auto& venue : level.venues()) {
          venue_quantities[venue.venue_name()] += venue.quantity();
        }
        idx++;
      }
      
      if (accumulated_quantity > 0.0) {
        auto* level = is_bid ? output->add_bids() : output->add_asks();
        level->set_price(target_price);
        level->set_quantity(accumulated_quantity);
        for (const auto& [venue_name, qty] : venue_quantities) {
          auto* venue = level->add_venues();
          venue->set_venue_name(venue_name);
          venue->set_quantity(qty);
        }
      }
    }
  };
  
  if (has_bid) {
    process_price_band(true);
  }
  if (has_ask) {
    process_price_band(false);
  }
  
  *output->mutable_venue_timestamps() = full_book.venue_timestamps();
}

void OrderBookResampler::ApplyVolumeBand(
    const ::herm::market_data::OrderBook& full_book,
    const std::vector<double>& notional_volumes,
    ::herm::market_data::OrderBook* output) {
  
  auto bids = full_book.bids();
  auto asks = full_book.asks();
  
  // Sort notional volumes in ascending order for accumulation
  auto sorted_volumes = notional_volumes;
  std::sort(sorted_volumes.begin(), sorted_volumes.end());
  
  // Helper lambda to process volume bands (bids or asks)
  auto process_volume_band = [&](bool is_bid) {
    const auto& levels = is_bid ? bids : asks;
    if (levels.empty()) {
      return;
    }
    
    double cumulative_notional = 0.0;
    double cumulative_quantity = 0.0;
    std::map<std::string, double> venue_quantities;
    size_t idx = 0;
    
    for (double target_notional : sorted_volumes) {
      // Continue accumulating from where we left off
      while (idx < static_cast<size_t>(levels.size()) && cumulative_notional < target_notional) {
        const auto& level = levels[idx];
        double level_notional = level.price() * level.quantity();
        
        if (cumulative_notional + level_notional >= target_notional) {
          // Partial level consumption
          double remaining_notional = target_notional - cumulative_notional;
          double partial_quantity = remaining_notional / level.price();
          
          cumulative_notional += remaining_notional;
          cumulative_quantity += partial_quantity;
          
          // Add partial venue quantities
          const auto& all_levels = is_bid ? full_book.bids() : full_book.asks();
          for (const auto& book_level : all_levels) {
            if (std::abs(book_level.price() - level.price()) < 0.0001) {
              double venue_ratio = partial_quantity / level.quantity();
              for (const auto& venue : book_level.venues()) {
                venue_quantities[venue.venue_name()] += venue.quantity() * venue_ratio;
              }
              break;
            }
          }
          break;  // Don't increment idx, we may need the remainder for next target
        } else {
          // Full level consumption
          cumulative_notional += level_notional;
          cumulative_quantity += level.quantity();
          
          // Add full venue quantities
          const auto& all_levels = is_bid ? full_book.bids() : full_book.asks();
          for (const auto& book_level : all_levels) {
            if (std::abs(book_level.price() - level.price()) < 0.0001) {
              for (const auto& venue : book_level.venues()) {
                venue_quantities[venue.venue_name()] += venue.quantity();
              }
              break;
            }
          }
          idx++;
        }
      }
      
      if (cumulative_notional >= target_notional && cumulative_quantity > 0.0) {
        double vwap_price = cumulative_notional / cumulative_quantity;
        auto* level = is_bid ? output->add_bids() : output->add_asks();
        level->set_price(vwap_price);
        level->set_quantity(cumulative_quantity);
        for (const auto& [venue_name, qty] : venue_quantities) {
          auto* venue = level->add_venues();
          venue->set_venue_name(venue_name);
          venue->set_quantity(qty);
        }
      }
    }
  };
  
  process_volume_band(true);   // Process bids
  process_volume_band(false);  // Process asks
  
  *output->mutable_venue_timestamps() = full_book.venue_timestamps();
}

}  // namespace market_data
}  // namespace herm
