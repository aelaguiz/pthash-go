.PHONY: build test bench fmt vet clean all simple-e2e build-simple-e2e

BINARY_NAME_BUILD=build
BINARY_NAME_EXAMPLE=example
CMD_DIR=./cmd

all: fmt vet build test

build:
	@echo "Building cmd/build..."
	go build -o $(BINARY_NAME_BUILD) $(CMD_DIR)/build
	@echo "Building cmd/example..."
	go build -o $(BINARY_NAME_EXAMPLE) $(CMD_DIR)/example
	@echo "Building all packages..."
	go build ./...

test:
	@echo "Running tests..."
	go test ./... -v

build-simple-e2e:
	@echo "Building simple end-to-end example with race detection..."
	go build -race -o simple_e2e ./examples/simple_e2e

simple-e2e: build-simple-e2e
	@echo "Running simple end-to-end example..."
	./simple_e2e

# Run tests with race detector
test-race:
	@echo "Running tests with race detector..."
	go test -race ./...

# Run tests with short flag for quicker testing
test-short:
	@echo "Running short tests..."
	go test -short ./...

# Run benchmarks
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
