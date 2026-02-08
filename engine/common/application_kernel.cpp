#include "application_kernel.hpp"

#include <CLI/CLI.hpp>
#include <absl/log/log_sink.h>
#include <absl/log/log_sink_registry.h>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <nlohmann/json.hpp>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>
#include <sstream>

namespace herm {
namespace engine {
namespace common {

ApplicationKernel* ApplicationKernel::instance_ = nullptr;

//=== Constructors & Destructor ===
ApplicationKernel::ApplicationKernel() 
    : app_name_("herm_app"),
      instrument_registry_(InstrumentRegistry::GetInstance()) {
  instance_ = this;
}

ApplicationKernel::~ApplicationKernel() {
  Shutdown();
  if (instance_ == this) {
    instance_ = nullptr;
  }
}

//=== Main Lifecycle ===

bool ApplicationKernel::Initialize(int argc, char** argv) {
  try {
    // Parse command-line arguments
    std::string config_file = ParseCommandLineArguments(argc, argv);
    
    // Load configuration
    LoadConfiguration(config_file);
    
    // Initialize logging (must be after config is loaded)
    InitializeLogging();
    
    // Load instrument registry
    LoadInstrumentRegistry(config_file);
    
    // Print startup information
    PrintStartupInfo();
    
    // Initialize CPU pinning manager
    if (!InitializeCPUPinning()) {
      return false;
    }
    
    // Setup signal handlers
    SetupSignalHandlers();
    
    // Initialize REST server
    InitializeRestServer();

    // Call OnInitialize hook (allows derived classes to do custom initialization)
    try {
      OnInitialize();
    } catch (const std::exception& e) {
      SPDLOG_ERROR("OnInitialize hook failed: {}", e.what());
      return false;
    }
    
  } catch (const std::exception& e) {
    SPDLOG_ERROR("Initialization failed: {}", e.what());
    return false;
  }
  
  return true;
}

std::string ApplicationKernel::ParseCommandLineArguments(int argc, char** argv) {
  CLI::App app{"herm Application"};
  std::string custom_config_file;
  app.add_option("--config_file", custom_config_file, "Path to configuration file")
     ->required()
     ->check(CLI::ExistingFile);
  
  try {
    app.parse(argc, argv);
  } catch (const CLI::ParseError& e) {
    // Re-throw with the actual CLI11 error message
    throw std::runtime_error(std::string("Command-line parsing error: ") + e.what());
  }
  
  return custom_config_file;
}

void ApplicationKernel::LoadConfiguration(const std::string& config_file) {
  loaded_config_file_ = config_file;  // Store the actual config file path
  config_.LoadFromFile(config_file);
}

void ApplicationKernel::InitializeLogging() {
  // Get logging configuration from config
  std::string log_file_path = config_.GetString("app.log.file", "logs/app.log");
  std::string log_level_str = config_.GetString("app.log.level", "info");
  
  // Parse log level
  spdlog::level::level_enum level = spdlog::level::info;
  if (log_level_str == "trace") {
    level = spdlog::level::trace;
  } else if (log_level_str == "debug") {
    level = spdlog::level::debug;
  } else if (log_level_str == "info") {
    level = spdlog::level::info;
  } else if (log_level_str == "warn" || log_level_str == "warning") {
    level = spdlog::level::warn;
  } else if (log_level_str == "error") {
    level = spdlog::level::err;
  } else if (log_level_str == "critical") {
    level = spdlog::level::critical;
  } else if (log_level_str == "off") {
    level = spdlog::level::off;
  }
  
  // Ensure log directory exists
  std::string path_str = log_file_path;
  try {
    std::filesystem::path p(log_file_path);
    if (!p.is_absolute()) {
      p = std::filesystem::absolute(p);
    }
    path_str = p.string();
    if (p.has_parent_path()) {
      std::filesystem::create_directories(p.parent_path());
    }
    
    // Create file and console sinks
    auto file_sink = std::make_shared<spdlog::sinks::basic_file_sink_mt>(path_str, true);
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    std::vector<spdlog::sink_ptr> sinks{file_sink, console_sink};
    
    // Create main logger
    auto logger = std::make_shared<spdlog::logger>("herm", sinks.begin(), sinks.end());
    logger->set_level(level);
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v [%s:%#]");
    logger->flush_on(level);
    spdlog::set_default_logger(logger);
    spdlog::set_level(level);
    
    // Create gRPC logger (file only)
    auto grpc_logger = std::make_shared<spdlog::logger>("grpc", file_sink);
    grpc_logger->set_level(level);
    grpc_logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v [%s:%#]");
    grpc_logger->flush_on(level);
    spdlog::register_logger(grpc_logger);
    
    SPDLOG_INFO("Logging to file: {} with level: {} {}", path_str, log_level_str, 
                "[" __FILE__ ":" + std::to_string(__LINE__) + "]");
  } catch (const std::exception& e) {
    std::fprintf(stderr, "[herm] InitLogging failed (path=%s): %s\n", path_str.c_str(), e.what());
    // Fallback to console only
    spdlog::set_default_logger(spdlog::stdout_color_mt("herm"));
    spdlog::set_level(level);
  } catch (...) {
    std::fprintf(stderr, "[herm] InitLogging failed (path=%s): unknown exception\n", path_str.c_str());
    spdlog::set_default_logger(spdlog::stdout_color_mt("herm"));
    spdlog::set_level(level);
  }
  
  // Configure gRPC to use spdlog
  SetGrpcLogToSpdlog();
  
  SPDLOG_INFO("Logging initialized from config: {}", loaded_config_file_);
}

void ApplicationKernel::SetGrpcLogToSpdlog() {
  // Custom log sink that redirects gRPC/Abseil logs to spdlog
  class SpdlogLogSink : public absl::LogSink {
   public:
    void Send(const absl::LogEntry& entry) override {
      std::shared_ptr<spdlog::logger> logger = spdlog::get("grpc");
      if (!logger) return;
      std::string msg(entry.text_message_with_prefix().data(),
                      entry.text_message_with_prefix().size());
      switch (entry.log_severity()) {
        case absl::LogSeverity::kInfo:
          logger->info("[grpc] {}", msg);
          break;
        case absl::LogSeverity::kWarning:
          logger->warn("[grpc] {}", msg);
          break;
        case absl::LogSeverity::kError:
          logger->error("[grpc] {}", msg);
          break;
        case absl::LogSeverity::kFatal:
          logger->critical("[grpc] {}", msg);
          break;
        default:
          logger->debug("[grpc] {}", msg);
          break;
      }
    }
  };
  
  static SpdlogLogSink sink;
  absl::AddLogSink(&sink);
  absl::SetStderrThreshold(absl::LogSeverityAtLeast::kFatal);
}

void ApplicationKernel::LoadInstrumentRegistry(const std::string& /* config_file */) {
  // Get instruments CSV path from config, with default fallback
  std::string csv_path_str = config_.GetString("app.instrument_registry", "static/instruments.csv");
  
  // Try the configured/default path first
  std::filesystem::path csv_path(csv_path_str);
  
  if (std::filesystem::exists(csv_path)) {
    if (!instrument_registry_.LoadFromCSV(csv_path.string())) {
      SPDLOG_ERROR("Failed to load instruments from CSV: {}", csv_path.string());
    }
  } else {
    SPDLOG_ERROR("Instruments CSV file not found at: {}", csv_path.string());
  }
}

void ApplicationKernel::PrintStartupInfo() {
  SPDLOG_INFO("Starting application: {}", app_name_);
  config_.PrintAllConfig();
}

bool ApplicationKernel::InitializeCPUPinning() {
  try {
    cpu_pinning_manager_ = std::make_unique<CPUPinningManager>(config_);
    cpu_pinning_manager_->Initialize();
    return true;
  } catch (const std::exception& e) {
    SPDLOG_ERROR("CPU pinning initialization failed: {}", e.what());
    return false;
  }
}

int ApplicationKernel::Run(int argc, char** argv) {
  
  if (!Initialize(argc, argv)) {
    return 1;
  }

  // Set CPU pinning manager on event thread before starting
  if (cpu_pinning_manager_) {
    event_thread_.SetCPUPinningManager(cpu_pinning_manager_.get());
  }

  // Start worker thread
  event_thread_.Start();
  
  // Start application
  SPDLOG_INFO("ApplicationKernel: Starting application");
  running_ = true;
  try {
    OnStart();
  } catch (const std::exception& e) {
    SPDLOG_ERROR("Start failed: {}", e.what());
    Shutdown();
    return 1;
  }
  
  SPDLOG_INFO("Application {} started", app_name_);
  
  while (running_.load()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  SPDLOG_INFO("Shutdown signal received");
  
  // Stop
  OnStop();
  
  // Shutdown
  Shutdown();
  
  SPDLOG_INFO("Application {} stopped", app_name_);
  return 0;
}

//=== Task Posting ===
void ApplicationKernel::Post(std::function<void()> task) {
  event_thread_.Post(std::move(task));
}

void ApplicationKernel::PostDelayed(std::function<void()> task, std::chrono::milliseconds delay) {
  event_thread_.PostDelayed(std::move(task), delay);
}

int ApplicationKernel::SchedulePeriodic(std::function<void()> task, std::chrono::milliseconds interval) {
  return event_thread_.SchedulePeriodic(std::move(task), interval);
}

void ApplicationKernel::CancelPeriodic(int task_id) {
  event_thread_.CancelPeriodic(task_id);
}

//=== Private Methods ===
void ApplicationKernel::SetupSignalHandlers() {
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);
}

void ApplicationKernel::SignalHandler(int signal) {
  if (instance_ && (signal == SIGINT || signal == SIGTERM)) {
    instance_->running_.store(false, std::memory_order_release);
    instance_->shutdown_cv_.notify_all();
  }
}

void ApplicationKernel::InitializeRestServer() {
  // Configure REST server
  rest_server_.SetAppName(app_name_);
  
  // Set up callbacks for standard endpoints
  rest_server_.SetStatusCallback([this]() {
    SPDLOG_INFO("Setting status callback triggered, running: {}", this->running_.load());
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    oss << R"({"app_name":")" << app_name_ 
        << R"(","uptime_seconds":)" << rest_server_.GetUptime().count()
        << R"(,"state":")" << (this->running_.load() ? "running" : "stopped") << R"("})";
    return oss.str();
  });
  
  rest_server_.SetMetricsCallback([this]() {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);
    oss << R"({"uptime_seconds":)" << rest_server_.GetUptime().count() << "}";
    return oss.str();
  });
  
  rest_server_.SetReloadConfigCallback([this]() {
    if (!loaded_config_file_.empty()) {
      return config_.LoadFromFile(loaded_config_file_);
    } else {
      std::string config_dir = ConfigManager::GetConfigDir();
      std::string config_file = config_dir + "/" + app_name_ + ".json";
      return config_.LoadFromFile(config_file);
    }
  });
  
  rest_server_.SetStopCallback([this]() {
    running_ = false;
  });
  
  // Start REST server if configured
  // Read rest_port from app.rest_port (default to 0 = disabled)
  int rest_port = config_.GetInt("app.rest_port", 0);
  std::string rest_address = "0.0.0.0";  // Always bind to 0.0.0.0
  
  SPDLOG_INFO("REST server config: address={}, port={}", rest_address, rest_port);
  
  if (rest_port > 0) {
    if (rest_server_.Start(rest_address, static_cast<uint16_t>(rest_port))) {
      SPDLOG_INFO("REST server started on {}:{}", rest_address, rest_port);
    } else {
      SPDLOG_WARN("Failed to start REST server on {}:{}", rest_address, rest_port);
    }
  } else {
    SPDLOG_INFO("REST server disabled (port=0 or not configured)");
  }
}

void ApplicationKernel::Shutdown() {
  // Use a separate shutdown flag to ensure we only shutdown once
  static std::atomic<bool> shutdown_called{false};
  if (shutdown_called.exchange(true)) {
    return;  // Already shutting down
  }
  
  // Set running to false if not already
  running_ = false;
  shutdown_cv_.notify_all();
  
  SPDLOG_INFO("Shutting down application: {}", app_name_);
  
  // Stop REST server
  rest_server_.Stop();
  
  // Stop worker thread with graceful shutdown
  // The thread will process remaining tasks before stopping
  SPDLOG_INFO("Stopping worker thread...");
  event_thread_.Stop();
  SPDLOG_INFO("Worker thread stopped");
  
  // Call shutdown hook
  try {
    OnShutdown();
  } catch (const std::exception& e) {
    SPDLOG_ERROR("Shutdown hook exception: {}", e.what());
  }
}

}  // namespace common
}  // namespace engine
}  // namespace herm
