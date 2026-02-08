#pragma once

#include "config_manager.hpp"
#include "cpu_pinning.hpp"
#include "event_thread.hpp"
#include "instrument_registry.hpp"
#include "rest_server.hpp"
#include <atomic>
#include <condition_variable>
#include <csignal>
#include <memory>
#include <string>

namespace herm {
namespace engine {
namespace common {

/**
 * @brief Base application framework providing common infrastructure.
 * 
 * ApplicationKernel provides a standardized lifecycle for applications:
 * 1. Initialize: Load config, setup logging, create threads
 * 2. Start: Begin processing (derived class logic)
 * 3. Run: Main event loop with signal handling
 * 4. Stop: Graceful shutdown
 * 
 * Features:
 * - Configuration management (JSON)
 * - Structured logging (spdlog)
 * - Event-driven task processing (EventThread)
 * - REST server with health endpoints
 * - CPU core pinning for performance
 * - Signal handling (SIGINT, SIGTERM)
 * 
 * Usage:
 * ```cpp
 * class MyApp : public ApplicationKernel {
 *  protected:
 *   void OnStart() override { // Start your services }
 *   void OnStop() override { // Stop your services }
 * };
 * ```
 */
class ApplicationKernel {
 public:
  //=== Constructors & Destructor ===
  /** @brief Constructs ApplicationKernel with default settings. */
  ApplicationKernel();
  
  /** @brief Virtual destructor for proper cleanup of derived classes. */
  virtual ~ApplicationKernel();

  //=== Deleted Copy Operations ===
  ApplicationKernel(const ApplicationKernel&) = delete;
  ApplicationKernel& operator=(const ApplicationKernel&) = delete;

  //=== Move Operations ===
  ApplicationKernel(ApplicationKernel&&) = default;
  ApplicationKernel& operator=(ApplicationKernel&&) = default;

  //=== Main Lifecycle ===
  /**
   * @brief Main entry point for the application.
   * 
   * Orchestrates full lifecycle: parse args, load config, initialize,
   * start, run event loop, handle signals, stop, shutdown.
   * @param argc Command line argument count.
   * @param argv Command line arguments.
   * @return Exit code (0 for success, non-zero for error).
   */
  int Run(int argc, char** argv);

  //=== Task Posting ===
  /** @brief Post immediate task to worker thread. */
  void Post(std::function<void()> task);

  /** @brief Post delayed task to worker thread. */
  void PostDelayed(std::function<void()> task, std::chrono::milliseconds delay);

  /**
   * @brief Schedule periodic task on worker thread.
   * @return Task ID for cancellation.
   */
  int SchedulePeriodic(std::function<void()> task, std::chrono::milliseconds interval);
  
  /** @brief Cancel a periodic task by ID. */
  void CancelPeriodic(int task_id);

  //=== Accessors ===
  /** @brief Get configuration manager. */
  ConfigManager& GetConfig() { return config_; }
  const ConfigManager& GetConfig() const { return config_; }

  /** @brief Get application name (for logging). */
  const std::string& GetAppName() const { return app_name_; }
  void SetAppName(const std::string& name) { app_name_ = name; }

  /** @brief Get REST server for custom endpoint registration. */
  RestServer& GetRestServer() { return rest_server_; }
  const RestServer& GetRestServer() const { return rest_server_; }

  /** @brief Get worker thread for task processing. */
  EventThread& GetEventThread() { return event_thread_; }
  const EventThread& GetEventThread() const { return event_thread_; }

  /** @brief Get instrument registry for symbol lookups. */
  InstrumentRegistry& GetInstrumentRegistry() { return instrument_registry_; }
  const InstrumentRegistry& GetInstrumentRegistry() const { return instrument_registry_; }

  /** @brief Get CPU pinning manager (nullptr if disabled). */
  CPUPinningManager* GetCPUPinningManager() { return cpu_pinning_manager_.get(); }

 protected:
  //=== Lifecycle Hooks (Override in Derived Classes) ===
  /** @brief Called after config loaded, before threads start. */
  virtual void OnInitialize() {}
  
  /** @brief Called when application starts (begin processing). */
  virtual void OnStart() {}
  
  /** @brief Called when application stops (end processing). */
  virtual void OnStop() {}
  
  /** @brief Called during final cleanup. */
  virtual void OnShutdown() {}

 private:
  //=== Private Members ===
  std::string app_name_;                              ///< Application name
  ConfigManager config_;                              ///< Configuration manager
  EventThread event_thread_;                          ///< Worker thread for async tasks
  RestServer rest_server_;                            ///< HTTP REST server
  InstrumentRegistry& instrument_registry_;           ///< Global instrument registry
  std::unique_ptr<CPUPinningManager> cpu_pinning_manager_;  ///< CPU core manager
  
  std::atomic<bool> running_{false};                  ///< Running state flag
  std::mutex shutdown_mutex_;                         ///< Protects shutdown CV
  std::condition_variable shutdown_cv_;               ///< Signals shutdown event
  std::string loaded_config_file_;                    ///< Path to loaded config
  static ApplicationKernel* instance_;                ///< Singleton for signal handler

  //=== Private Methods ===
  /** @brief Initialize the kernel (called by Run). */
  bool Initialize(int argc, char** argv);
  
  /** @brief Register SIGINT/SIGTERM handlers. */
  void SetupSignalHandlers();
  
  /** @brief Static signal handler callback. */
  static void SignalHandler(int signal);
  
  /** @brief Perform graceful shutdown. */
  void Shutdown();
  
  /** @brief Initialize REST server with health endpoints. */
  void InitializeRestServer();
  
  //=== Initialization Helpers ===
  /** @brief Parse --config argument from command line. */
  std::string ParseCommandLineArguments(int argc, char** argv);
  
  /** @brief Load JSON configuration from file. */
  void LoadConfiguration(const std::string& config_file);
  
  /** @brief Setup spdlog with configured level and format. */
  void InitializeLogging();
  
  /** @brief Configure gRPC/Abseil to use spdlog. */
  void SetGrpcLogToSpdlog();
  
  /** @brief Load instrument registry from config. */
  void LoadInstrumentRegistry(const std::string& config_file);
  
  /** @brief Initialize CPU pinning if enabled. */
  bool InitializeCPUPinning();
  
  /** @brief Print startup banner with config info. */
  void PrintStartupInfo();
};

}  // namespace common
}  // namespace engine
}  // namespace herm
