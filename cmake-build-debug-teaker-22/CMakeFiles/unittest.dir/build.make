# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.10

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/miao/myproject/P-ART

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/miao/myproject/P-ART/cmake-build-debug-teaker-22

# Include any dependencies generated for this target.
include CMakeFiles/unittest.dir/depend.make

# Include the progress variables for this target.
include CMakeFiles/unittest.dir/progress.make

# Include the compile flags for this target's objects.
include CMakeFiles/unittest.dir/flags.make

CMakeFiles/unittest.dir/test/test.cpp.o: CMakeFiles/unittest.dir/flags.make
CMakeFiles/unittest.dir/test/test.cpp.o: ../test/test.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/miao/myproject/P-ART/cmake-build-debug-teaker-22/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object CMakeFiles/unittest.dir/test/test.cpp.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/unittest.dir/test/test.cpp.o -c /home/miao/myproject/P-ART/test/test.cpp

CMakeFiles/unittest.dir/test/test.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/unittest.dir/test/test.cpp.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/miao/myproject/P-ART/test/test.cpp > CMakeFiles/unittest.dir/test/test.cpp.i

CMakeFiles/unittest.dir/test/test.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/unittest.dir/test/test.cpp.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/miao/myproject/P-ART/test/test.cpp -o CMakeFiles/unittest.dir/test/test.cpp.s

CMakeFiles/unittest.dir/test/test.cpp.o.requires:

.PHONY : CMakeFiles/unittest.dir/test/test.cpp.o.requires

CMakeFiles/unittest.dir/test/test.cpp.o.provides: CMakeFiles/unittest.dir/test/test.cpp.o.requires
	$(MAKE) -f CMakeFiles/unittest.dir/build.make CMakeFiles/unittest.dir/test/test.cpp.o.provides.build
.PHONY : CMakeFiles/unittest.dir/test/test.cpp.o.provides

CMakeFiles/unittest.dir/test/test.cpp.o.provides.build: CMakeFiles/unittest.dir/test/test.cpp.o


CMakeFiles/unittest.dir/test/test_correctness.cpp.o: CMakeFiles/unittest.dir/flags.make
CMakeFiles/unittest.dir/test/test_correctness.cpp.o: ../test/test_correctness.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/miao/myproject/P-ART/cmake-build-debug-teaker-22/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Building CXX object CMakeFiles/unittest.dir/test/test_correctness.cpp.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/unittest.dir/test/test_correctness.cpp.o -c /home/miao/myproject/P-ART/test/test_correctness.cpp

CMakeFiles/unittest.dir/test/test_correctness.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/unittest.dir/test/test_correctness.cpp.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/miao/myproject/P-ART/test/test_correctness.cpp > CMakeFiles/unittest.dir/test/test_correctness.cpp.i

CMakeFiles/unittest.dir/test/test_correctness.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/unittest.dir/test/test_correctness.cpp.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/miao/myproject/P-ART/test/test_correctness.cpp -o CMakeFiles/unittest.dir/test/test_correctness.cpp.s

CMakeFiles/unittest.dir/test/test_correctness.cpp.o.requires:

.PHONY : CMakeFiles/unittest.dir/test/test_correctness.cpp.o.requires

CMakeFiles/unittest.dir/test/test_correctness.cpp.o.provides: CMakeFiles/unittest.dir/test/test_correctness.cpp.o.requires
	$(MAKE) -f CMakeFiles/unittest.dir/build.make CMakeFiles/unittest.dir/test/test_correctness.cpp.o.provides.build
.PHONY : CMakeFiles/unittest.dir/test/test_correctness.cpp.o.provides

CMakeFiles/unittest.dir/test/test_correctness.cpp.o.provides.build: CMakeFiles/unittest.dir/test/test_correctness.cpp.o


CMakeFiles/unittest.dir/test/test_epoch.cpp.o: CMakeFiles/unittest.dir/flags.make
CMakeFiles/unittest.dir/test/test_epoch.cpp.o: ../test/test_epoch.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/miao/myproject/P-ART/cmake-build-debug-teaker-22/CMakeFiles --progress-num=$(CMAKE_PROGRESS_3) "Building CXX object CMakeFiles/unittest.dir/test/test_epoch.cpp.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/unittest.dir/test/test_epoch.cpp.o -c /home/miao/myproject/P-ART/test/test_epoch.cpp

CMakeFiles/unittest.dir/test/test_epoch.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/unittest.dir/test/test_epoch.cpp.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/miao/myproject/P-ART/test/test_epoch.cpp > CMakeFiles/unittest.dir/test/test_epoch.cpp.i

CMakeFiles/unittest.dir/test/test_epoch.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/unittest.dir/test/test_epoch.cpp.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/miao/myproject/P-ART/test/test_epoch.cpp -o CMakeFiles/unittest.dir/test/test_epoch.cpp.s

CMakeFiles/unittest.dir/test/test_epoch.cpp.o.requires:

.PHONY : CMakeFiles/unittest.dir/test/test_epoch.cpp.o.requires

CMakeFiles/unittest.dir/test/test_epoch.cpp.o.provides: CMakeFiles/unittest.dir/test/test_epoch.cpp.o.requires
	$(MAKE) -f CMakeFiles/unittest.dir/build.make CMakeFiles/unittest.dir/test/test_epoch.cpp.o.provides.build
.PHONY : CMakeFiles/unittest.dir/test/test_epoch.cpp.o.provides

CMakeFiles/unittest.dir/test/test_epoch.cpp.o.provides.build: CMakeFiles/unittest.dir/test/test_epoch.cpp.o


CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.o: CMakeFiles/unittest.dir/flags.make
CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.o: ../test/test_nvm_mgr.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/home/miao/myproject/P-ART/cmake-build-debug-teaker-22/CMakeFiles --progress-num=$(CMAKE_PROGRESS_4) "Building CXX object CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.o"
	/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.o -c /home/miao/myproject/P-ART/test/test_nvm_mgr.cpp

CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.i"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /home/miao/myproject/P-ART/test/test_nvm_mgr.cpp > CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.i

CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.s"
	/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /home/miao/myproject/P-ART/test/test_nvm_mgr.cpp -o CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.s

CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.o.requires:

.PHONY : CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.o.requires

CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.o.provides: CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.o.requires
	$(MAKE) -f CMakeFiles/unittest.dir/build.make CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.o.provides.build
.PHONY : CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.o.provides

CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.o.provides.build: CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.o


# Object files for target unittest
unittest_OBJECTS = \
"CMakeFiles/unittest.dir/test/test.cpp.o" \
"CMakeFiles/unittest.dir/test/test_correctness.cpp.o" \
"CMakeFiles/unittest.dir/test/test_epoch.cpp.o" \
"CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.o"

# External object files for target unittest
unittest_EXTERNAL_OBJECTS =

unittest: CMakeFiles/unittest.dir/test/test.cpp.o
unittest: CMakeFiles/unittest.dir/test/test_correctness.cpp.o
unittest: CMakeFiles/unittest.dir/test/test_epoch.cpp.o
unittest: CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.o
unittest: CMakeFiles/unittest.dir/build.make
unittest: libIndexes.a
unittest: /usr/lib/x86_64-linux-gnu/libtbb.so
unittest: CMakeFiles/unittest.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --bold --progress-dir=/home/miao/myproject/P-ART/cmake-build-debug-teaker-22/CMakeFiles --progress-num=$(CMAKE_PROGRESS_5) "Linking CXX executable unittest"
	$(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/unittest.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
CMakeFiles/unittest.dir/build: unittest

.PHONY : CMakeFiles/unittest.dir/build

CMakeFiles/unittest.dir/requires: CMakeFiles/unittest.dir/test/test.cpp.o.requires
CMakeFiles/unittest.dir/requires: CMakeFiles/unittest.dir/test/test_correctness.cpp.o.requires
CMakeFiles/unittest.dir/requires: CMakeFiles/unittest.dir/test/test_epoch.cpp.o.requires
CMakeFiles/unittest.dir/requires: CMakeFiles/unittest.dir/test/test_nvm_mgr.cpp.o.requires

.PHONY : CMakeFiles/unittest.dir/requires

CMakeFiles/unittest.dir/clean:
	$(CMAKE_COMMAND) -P CMakeFiles/unittest.dir/cmake_clean.cmake
.PHONY : CMakeFiles/unittest.dir/clean

CMakeFiles/unittest.dir/depend:
	cd /home/miao/myproject/P-ART/cmake-build-debug-teaker-22 && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/miao/myproject/P-ART /home/miao/myproject/P-ART /home/miao/myproject/P-ART/cmake-build-debug-teaker-22 /home/miao/myproject/P-ART/cmake-build-debug-teaker-22 /home/miao/myproject/P-ART/cmake-build-debug-teaker-22/CMakeFiles/unittest.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : CMakeFiles/unittest.dir/depend
