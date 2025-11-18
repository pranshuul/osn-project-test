CC = gcc
CFLAGS = -Wall -Wextra -g -Iinclude
LDFLAGS = -lpthread

# Directories
SRC_DIR = src
INC_DIR = include
OBJ_DIR = obj
BIN_DIR = bin
DATA_DIR = data

# Source files
COMMON_SRC = $(SRC_DIR)/common.c $(SRC_DIR)/hashmap.c $(SRC_DIR)/sentence_parser.c
NM_SRC = $(SRC_DIR)/name_server.c
SS_SRC = $(SRC_DIR)/storage_server.c
CLIENT_SRC = $(SRC_DIR)/client.c

# Object files
COMMON_OBJ = $(patsubst $(SRC_DIR)/%.c,$(OBJ_DIR)/%.o,$(COMMON_SRC))
NM_OBJ = $(OBJ_DIR)/name_server.o
SS_OBJ = $(OBJ_DIR)/storage_server.o
CLIENT_OBJ = $(OBJ_DIR)/client.o

# Executables
NM_BIN = $(BIN_DIR)/name_server
SS_BIN = $(BIN_DIR)/storage_server
CLIENT_BIN = $(BIN_DIR)/client

# Default target
all: dirs $(NM_BIN) $(SS_BIN) $(CLIENT_BIN)

# Create directories
dirs:
	@mkdir -p $(OBJ_DIR) $(BIN_DIR)
	@mkdir -p $(DATA_DIR)/files $(DATA_DIR)/metadata $(DATA_DIR)/undo

# Name Server
$(NM_BIN): $(NM_OBJ) $(COMMON_OBJ)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "Built Name Server"

# Storage Server
$(SS_BIN): $(SS_OBJ) $(COMMON_OBJ)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "Built Storage Server"

# Client
$(CLIENT_BIN): $(CLIENT_OBJ) $(COMMON_OBJ)
	$(CC) $(CFLAGS) -o $@ $^ $(LDFLAGS)
	@echo "Built Client"

# Compile object files
$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	@mkdir -p $(OBJ_DIR)
	$(CC) $(CFLAGS) -c -o $@ $<

# Clean build artifacts
clean:
	rm -rf $(OBJ_DIR) $(BIN_DIR)
	@echo "Cleaned build artifacts"

# Clean everything including data
cleanall: clean
	rm -rf $(DATA_DIR)
	@echo "Cleaned all files including data"

# Run targets
run-nm: $(NM_BIN)
	@echo "Starting Name Server..."
	@$(NM_BIN)

run-ss: $(SS_BIN)
	@echo "Starting Storage Server..."
	@$(SS_BIN)

run-client: $(CLIENT_BIN)
	@echo "Starting Client..."
	@$(CLIENT_BIN)

# Help
help:
	@echo "Available targets:"
	@echo "  all          - Build all components (default)"
	@echo "  clean        - Remove build artifacts"
	@echo "  cleanall     - Remove build artifacts and data files"
	@echo "  run-nm       - Build and run Name Server"
	@echo "  run-ss       - Build and run Storage Server"
	@echo "  run-client   - Build and run Client"
	@echo ""
	@echo "Usage:"
	@echo "  make              # Build everything"
	@echo "  make run-nm       # Run Name Server (terminal 1)"
	@echo "  make run-ss       # Run Storage Server (terminal 2)"
	@echo "  make run-client   # Run Client (terminal 3)"

.PHONY: all clean cleanall dirs run-nm run-ss run-client help
