#!/bin/bash

# Script to list all failing tests in the project
# Usage: ./scripts/list_failing_tests.sh [package_pattern]

# Set default package pattern if not provided
PACKAGE_PATTERN=${1:-"./..."}

echo "Running tests for pattern: $PACKAGE_PATTERN"
echo "Listing failing tests..."
echo "======================="

# Run tests and capture output
TEMP_FILE=$(mktemp)
BUILD_FAIL_FILE=$(mktemp)
TEST_OUTPUT=$(go test $PACKAGE_PATTERN -v -timeout 5s 2>&1)

# Check for build failures
echo "$TEST_OUTPUT" | grep -B1 "FAIL.*\[build failed\]" | grep -v "^--$" > "$BUILD_FAIL_FILE"
if [ -s "$BUILD_FAIL_FILE" ]; then
    echo "Build Failures Detected:"
    echo "======================="
    cat "$BUILD_FAIL_FILE"
    echo "======================="
    echo ""
fi

# Check for test failures
echo "$TEST_OUTPUT" | grep -A1 "^--- FAIL" | grep -v "===" | grep -v "^--$" | grep "FAIL:" | sed 's/--- FAIL: //' | sed 's/ (.*//' > "$TEMP_FILE"

# For each failing test, find the file it's in
while read -r test_name; do
    echo "Test: $test_name"
    grep -r "func $test_name" --include="*.go" ./lib/tests ./pokerai | sed 's/func //' | awk '{print "  File: " $1}'
    echo ""
done < "$TEMP_FILE"

# Generate a command to run only failing tests
if [ -s "$TEMP_FILE" ]; then
    # Properly format test names by trimming whitespace and creating a valid regex
    TEST_REGEX=$(awk '{gsub(/^[ \t]+|[ \t]+$/, ""); print}' "$TEMP_FILE" | tr '\n' '|' | sed 's/|$//')
    echo "======================="
    echo "Command to run only failing tests (copy and run this):"
    # Print without wrapping to make it easy to copy
    COMMAND="go test -v $PACKAGE_PATTERN -run=\"$TEST_REGEX\""
    printf "%s\n" "$COMMAND"
fi

# Clean up
rm "$TEMP_FILE"
rm "$BUILD_FAIL_FILE"

echo "======================="
echo "Test run complete."