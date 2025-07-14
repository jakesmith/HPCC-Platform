# GitHub Copilot Instructions for HPCC Platform

## Repository Structure

This repository contains **two distinct projects**:

1. **C++ Project** (this root folder): The main HPCC Platform system
2. **TypeScript/React Project** (`esp/src/`): ECL Watch web interface

## C++ Project (Root Folder)

### Build System
- **Build tool**: CMake with Ninja generator
- **Configuration**: Use options from `vcpkg-linux.code-workspace`
- **Key cmake arguments**: deduce cmake arguments from .vscode/settings.json

### Build Commands
- **Build directory** (<build-dir>): Extract the current setting from "cmake.buildDirectory" in .vscode/settings.json which is based on $buildType
```bash
# Configure the build
cmake -B <build-dir> -S . -G Ninja -DCONTAINERIZED=OFF -DUSE_OPTIONAL=OFF -DUSE_CPPUNIT=ON -DINCLUDE_PLUGINS=ON -DSUPPRESS_V8EMBED=ON -DSUPPRESS_REMBED=ON -DCMAKE_BUILD_TYPE=Debug

# Build
cmake --build <build-dir> --parallel

# Create package
cmake --build <build-dir> --parallel --target package
```

### Key Directories
- `common/` - Common utilities and libraries
- `dali/` - Dali distributed server components  
- `ecl/` - ECL compiler and language components
- `esp/` - Enterprise Services Platform (web services)
- `roxie/` - Roxie rapid data delivery engine
- `thorlcr/` - Thor large capacity resource (batch processing)
- `system/` - System-level components
- `rtl/` - Runtime library
- `devdoc/` - Developer documentation
- `testing/` - Test suites

### Code Style
- Follow the style guide in `devdoc/StyleGuide.md`
- Review guidelines in `devdoc/CodeReviews.md`
- Submission process in `devdoc/CodeSubmissions.md`
- Never add trailing whitespace, be careful when copying code from other sources
- Use Allman style for C++ code blocks
- Do not use brace curly blocks for single line blocks, unless they are nested
- Use camel case for variable names
- Use constexpr's for constances not macros
- Use Owned vs Linked for assigning ownership of objects
- Avoid default parameters in function declarations (overload methods with different prototypes instead)
- Use `#pragma once` for header guards
- Use %u for unsigned integers, %d for signed integers

### Development Workflow
- See `devdoc/Development.md` for testing and development guidelines
- Use pull requests with code reviews
- Target appropriate branch per `devdoc/VersionSupport.md`

## TypeScript/React Project

For the ECL Watch web interface, see the separate instructions in `esp/src/.github/instructions/general.instructions.md`.

## Documentation
- Contributing docs: `devdoc/docs/ContributeDocs.md`
- GitHub Copilot tips: `devdoc/userdoc/copilot/CopilotPromptTips.md`

## Code Reviews
- Follow the guidelines in `devdoc/CodeReviews.md`
- When reviewing code, pay particular attention to the following questions:
- Are there any efficiency concerns?
- Is the code thread safe?
- Could the code be refactored to improve maintainability and reuse?
- Are there any memory or resource leaks?
