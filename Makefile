.PHONY: build test bench fmt vet clean

BINARY_NAME_BUILD=build
BINARY_NAME_EXAMPLE=example
CMD_DIR=./cmd

build:
	@echo "Building cmd/build..."
	go build -o $(BINARY_NAME_BUILD) $(CMD_DIR)/build
	@echo "Building cmd/example..."
	go build -o $(BINARY_NAME_EXAMPLE) $(CMD_DIR)/example

# Placeholder test - will fail until tests exist
test:
	@echo "Running tests..."
	go test ./... -v

# Placeholder bench - will fail until benchmarks exist
bench:
	@echo "Running benchmarks..."
	go test ./... -bench=. -benchmem

fmt:
	@echo "Formatting code..."
	go fmt ./...

vet:
	@echo "Running go vet..."
	go vet ./...

clean:
	@echo "Cleaning..."
	go clean
	rm -f $(BINARY_NAME_BUILD) $(BINARY_NAME_EXAMPLE) *.bin *.log
