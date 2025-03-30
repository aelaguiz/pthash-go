#!/bin/bash

# Script to list all failing tests in the project
# Usage: ./scripts/list_failing_tests.sh [package_pattern] [max_output_lines]
# Tests are run with race detection enabled

# Set default package pattern if not provided
PACKAGE_PATTERN=${1:-"./..."}

# Set default max output lines if not provided
MAX_OUTPUT_LINES=${2:-100}

echo "Running tests for pattern: $PACKAGE_PATTERN with 30s timeout and race detection"
echo "Max output lines per failing test: $MAX_OUTPUT_LINES"
echo "======================="

# Run tests and capture output
TEMP_FILE=$(mktemp)
BUILD_FAIL_FILE=$(mktemp)
TIMEOUT_FAIL_FILE=$(mktemp)
PANIC_FAIL_FILE=$(mktemp)
PANIC_TEST_NAMES_FILE=$(mktemp)
PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
TIMEOUT_COUNT=0
PANIC_COUNT=0

# Use tee to display output in real-time while also capturing it
echo "Starting tests..."
TEST_OUTPUT=$(go test $PACKAGE_PATTERN -v -timeout 30s -race 2>&1 | tee /dev/tty)

# Count passing tests
PASS_COUNT=$(echo "$TEST_OUTPUT" | grep "^--- PASS" | wc -l)

# Count skipped tests
SKIP_COUNT=$(echo "$TEST_OUTPUT" | grep "^--- SKIP" | wc -l)

# Check for build failures
echo "$TEST_OUTPUT" | grep -B1 "FAIL.*\[build failed\]" | grep -v "^--$" > "$BUILD_FAIL_FILE"
if [ -s "$BUILD_FAIL_FILE" ]; then
    echo ""
    echo "Build Failures Detected:"
    echo "======================="
    cat "$BUILD_FAIL_FILE"
    echo "======================="
    echo ""
fi

# Extract timeout failures and add to failure list
echo "$TEST_OUTPUT" | grep -A5 "panic: test timed out after" > "$TIMEOUT_FAIL_FILE"
if [ -s "$TIMEOUT_FAIL_FILE" ]; then
    # Count timeout failures
    TIMEOUT_COUNT=$(echo "$TEST_OUTPUT" | grep -A5 "panic: test timed out after" | grep "running tests:" | wc -l)
    
    echo ""
    echo "Timeout Failures Detected:"
    echo "======================="
    
    # Extract the "running tests:" lines that show which tests timed out
    TIMEOUT_TESTS=$(echo "$TEST_OUTPUT" | grep -A5 "panic: test timed out after" | grep "running tests:" -A 2 | grep -v "running tests:" | grep -v "^--$" | sed 's/\s\+/ /' | sed 's/^ *//' | sed 's/ (.*)$//')
    
    # Only print the actual test names
    echo "$TIMEOUT_TESTS" | while read -r line; do
        if [[ "$line" == Test* ]]; then
            echo "$line"
        fi
    done
    
    echo "======================="
    
    # Show limited context around timeouts
    echo "Timeout Context (truncated to $MAX_OUTPUT_LINES lines per timeout):"
    echo "$TEST_OUTPUT" | grep -B5 -A$MAX_OUTPUT_LINES "panic: test timed out after" | head -n $MAX_OUTPUT_LINES
    echo "======================="
    echo ""
    
    # Add timeout test names to the list of failing tests
    echo "$TIMEOUT_TESTS" | grep "^Test" > "$TEMP_FILE"
fi

# Extract panics that are not timeout panics
# Look for panic markers in the output
echo "$TEST_OUTPUT" | grep -A10 "panic: " | grep -v "panic: test timed out after" > "$PANIC_FAIL_FILE"
if [ -s "$PANIC_FAIL_FILE" ]; then
    # Extract packages that had panics
    PANIC_PACKAGES=$(echo "$TEST_OUTPUT" | grep -B2 "panic: " | grep -v "panic: test timed out after" | grep "^==== " | sed 's/^==== \(.*\)/\1/' | sed 's/ ====//')
    
    # Try to find test names by getting the most recent "=== RUN " lines before each panic
    echo "$TEST_OUTPUT" | grep -B50 "panic: " | grep -v "panic: test timed out after" | grep "=== RUN " | sed 's/=== RUN   //' > "$PANIC_TEST_NAMES_FILE"
    
    # Count panic failures - each package with a panic counts as 1
    PANIC_COUNT=$(echo "$PANIC_PACKAGES" | wc -l)
    if [ "$PANIC_COUNT" -eq 0 ]; then
        PANIC_COUNT=1 # At least one panic if we detected a panic
    fi
    
    echo ""
    echo "Panic Failures Detected:"
    echo "======================="
    
    # Print the packages with panics
    if [ -n "$PANIC_PACKAGES" ]; then
        echo "$PANIC_PACKAGES" | while read -r package; do
            echo "Package: $package"
            # Extract panic details for this package, limiting output
            PACKAGE_PANIC=$(echo "$TEST_OUTPUT" | grep -A$MAX_OUTPUT_LINES "^==== $package ====" | grep -A$MAX_OUTPUT_LINES "panic: " | head -$MAX_OUTPUT_LINES)
            echo "$PACKAGE_PANIC"
            echo ""
        done
    else
        # If we didn't find package headers but did find panics, just show the panics (limited)
        PANIC_SNIPPET=$(cat "$PANIC_FAIL_FILE" | head -$MAX_OUTPUT_LINES)
        echo "$PANIC_SNIPPET"
        echo ""
    fi
    
    echo "======================="
    echo ""
    
    # Show the test names extracted from before the panic
    if [ -s "$PANIC_TEST_NAMES_FILE" ]; then
        # Get the last test that was running before the panic
        LAST_TEST=$(tail -n 1 "$PANIC_TEST_NAMES_FILE")
        
        # Extract the base test name (in case it's a subtest)
        if [[ "$LAST_TEST" == *"/"* ]]; then
            BASE_TEST=$(echo "$LAST_TEST" | cut -d'/' -f1)
            echo "Test that panicked: $LAST_TEST (base test: $BASE_TEST)"
            # Add both the full test name and the base test name to our list
            echo "$LAST_TEST" >> "$TEMP_FILE"
            echo "$BASE_TEST" >> "$TEMP_FILE"
        else
            echo "Test that panicked: $LAST_TEST"
            echo "$LAST_TEST" >> "$TEMP_FILE"
        fi
        echo ""
    fi
fi

# Check for test failures
echo "$TEST_OUTPUT" | grep -A1 "^--- FAIL" | grep -v "===" | grep -v "^--$" | grep "FAIL:" | sed 's/--- FAIL: //' | sed 's/ (.*//' >> "$TEMP_FILE"

# Count normal failures (excluding timeouts and panics)
NORMAL_FAIL_COUNT=$(echo "$TEST_OUTPUT" | grep "^--- FAIL" | wc -l)
FAIL_COUNT=$((NORMAL_FAIL_COUNT + TIMEOUT_COUNT + PANIC_COUNT))

# Filter duplicates from the temp file to avoid duplicates in failing test list
if [ -s "$TEMP_FILE" ]; then
    sort "$TEMP_FILE" | uniq > "${TEMP_FILE}.uniq"
    mv "${TEMP_FILE}.uniq" "$TEMP_FILE"
fi

# For each failing test, find the file it's in
echo ""
echo "Failed Tests:"
echo "======================="
while read -r test_name; do
    # Skip empty lines
    if [ -z "$test_name" ]; then
        continue
    fi
    
    echo "Test: $test_name"
    # Improve the grep to look for exact test function matches
    grep -r "func $test_name(" --include="*.go" ./internal ./pkg | sed 's/func //' | awk '{print "  File: " $1}'
    echo ""
done < "$TEMP_FILE"

# Extract and show limited output from failing tests
if [ -s "$TEMP_FILE" ]; then
    echo ""
    echo "Failing Test Output (Truncated):"
    echo "======================="
    
    while read -r test_name; do
        # Skip empty lines
        if [ -z "$test_name" ]; then
            continue
        fi
        
        echo "Test Output for: $test_name"
        echo "--------------------"
        
        # Handle both main tests and subtests by checking different patterns
        # First try exact test name
        TEST_OUTPUT_SECTION=$(echo "$TEST_OUTPUT" | awk -v test="$test_name" '
            # Look for exact test name or subtest containing this name
            $0 ~ "=== RUN   " test "$" || $0 ~ "=== RUN   " test "/" {flag=1; print; next}
            # Also match FAIL lines for this test
            $0 ~ "--- FAIL: " test " " || $0 ~ "--- FAIL: " test "/" {flag=1; print; next}
            # Stop when we see another test start or end
            flag && /^=== (RUN|PAUSE|CONT|PASS|FAIL|SKIP)/ && !($0 ~ test "/") {flag=0}
            # Print everything in between
            flag {print}
        ')
        
        # If we found output, show the last MAX_OUTPUT_LINES lines
        if [ -n "$TEST_OUTPUT_SECTION" ]; then
            TOTAL_LINES=$(echo "$TEST_OUTPUT_SECTION" | wc -l)
            if [ $TOTAL_LINES -gt $MAX_OUTPUT_LINES ]; then
                echo "$TEST_OUTPUT_SECTION" | tail -n $MAX_OUTPUT_LINES
                echo "... (output truncated, showing last $MAX_OUTPUT_LINES of $TOTAL_LINES lines)"
            else
                echo "$TEST_OUTPUT_SECTION"
            fi
        else
            # Try to find any related output by grepping for the test name
            TEST_RELATED_OUTPUT=$(echo "$TEST_OUTPUT" | grep -A$MAX_OUTPUT_LINES -B3 "$test_name" | head -$MAX_OUTPUT_LINES)
            if [ -n "$TEST_RELATED_OUTPUT" ]; then
                echo "$TEST_RELATED_OUTPUT"
                echo "... (output might be incomplete, showing grep context)"
            else
                echo "No specific output found for this test."
            fi
        fi
        
        echo ""
    done < "$TEMP_FILE"
    
    echo "======================="
fi

# Generate a command to run only failing tests
if [ -s "$TEMP_FILE" ]; then
    # Properly format test names by trimming whitespace and creating a valid regex
    TEST_REGEX=$(awk '{gsub(/^[ \t]+|[ \t]+$/, ""); print}' "$TEMP_FILE" | grep -v "^$" | tr '\n' '|' | sed 's/|$//')
    
    if [ -n "$TEST_REGEX" ]; then
        echo "======================="
        echo "Command to reproduce all failures (including panics):"
        # Print without wrapping to make it easy to copy
        COMMAND="go test -v $PACKAGE_PATTERN -run=\"$TEST_REGEX\" -timeout 30s -race"
        printf "%s\n" "$COMMAND"
    fi
# If no specific tests were identified but we had panics, suggest running the whole package
elif [ "$PANIC_COUNT" -gt 0 ]; then
    echo "======================="
    echo "Command to reproduce all failures (including panics):"
    # Print without wrapping to make it easy to copy
    printf "go test -v $PACKAGE_PATTERN -timeout 30s -race\n"
fi

# Print summary
echo ""
echo "Test Summary"
echo "======================="
echo "Passed:   $PASS_COUNT"
echo "Failed:   $FAIL_COUNT (including $TIMEOUT_COUNT timeouts and $PANIC_COUNT panics)"
echo "Skipped:  $SKIP_COUNT"
echo "Total:    $((PASS_COUNT + FAIL_COUNT + SKIP_COUNT))"
echo "======================="

# Clean up
rm "$TEMP_FILE"
rm "$BUILD_FAIL_FILE"
rm "$TIMEOUT_FAIL_FILE"
rm "$PANIC_FAIL_FILE"
rm "$PANIC_TEST_NAMES_FILE"

echo "Test run complete."