from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMakeDeps


class hermConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeDeps", "CMakeToolchain"

    def requirements(self):
        self.requires("grpc/1.65.0")
        self.requires("boost/1.84.0")
        self.requires("nlohmann_json/3.11.3")
        self.requires("spdlog/1.17.0")
        # Same range as grpc 1.65; for absl::LogSink (non-deprecated gRPC log redirect)
        self.requires("abseil/20240116.2")
        self.requires("gtest/1.14.0")
        self.requires("cli11/2.4.2")

    def configure(self):
        self.options["grpc"].shared = False
