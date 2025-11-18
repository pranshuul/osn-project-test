# NFS System Tests

This directory contains the test suite for the NFS (Network File System) implementation. The tests are organized into three main categories:

1. **Unit Tests**: Test individual components in isolation
2. **Integration Tests**: Test the interaction between components
3. **Performance Tests**: Measure system performance under various conditions

## Prerequisites

- Python 3.7+
- pip (Python package manager)
- The NFS system binaries (client, name_server, storage_server)

## Setup

1. Install the required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

2. Ensure the NFS system is built and the binaries are in the `bin` directory:
   ```bash
   make all
   ```

## Running Tests

### Using the Test Runner

The easiest way to run the tests is using the provided test runner:

```bash
# Run all tests
python3 tests/run_tests.py all

# Run only unit tests
python3 tests/run_tests.py unit

# Run only integration tests
python3 tests/run_tests.py integration

# Run only performance tests
python3 tests/run_tests.py performance

# Run tests with additional pytest options
python3 tests/run_tests.py all -v -s --log-level=DEBUG
```

### Directly with pytest

You can also run tests directly using pytest:

```bash
# Run all tests
pytest tests/

# Run a specific test file
pytest tests/unit/test_basic_operations.py

# Run a specific test function
pytest tests/unit/test_basic_operations.py::TestBasicOperations::test_create_and_read_file

# Run tests with coverage report
pytest --cov=src --cov-report=term-missing tests/
```

## Test Categories

### 1. Unit Tests

Test individual components in isolation:

- `test_basic_operations.py`: Basic file operations (create, read, write, delete)
- More unit tests will be added as the project evolves

### 2. Integration Tests

Test the interaction between components:

- `test_nfs_integration.py`: End-to-end tests for the NFS system
  - File operations workflow
  - Directory operations
  - Permission handling
  - Concurrent access

### 3. Performance Tests

Measure system performance under various conditions:

- `test_performance.py`:
  - File read/write performance with different file sizes
  - Concurrent read/write operations
  - System limits testing

## Test Data

Test data is stored in the `data/` directory:

- `data/files/`: Test files created during testing
- `data/metadata/`: File metadata and access control lists
- `data/checkpoints/`: File checkpoints for versioning

## Test Results

Test results and coverage reports are generated in the following directories:

- `logs/`: Log files from test runs
- `htmlcov/`: HTML coverage report (generated with `--cov-report=html`)
- `results/`: Performance test results in JSON format

## Writing New Tests

When adding new tests, follow these guidelines:

1. Place unit tests in `tests/unit/`
2. Place integration tests in `tests/integration/`
3. Place performance tests in `tests/performance/`
4. Use descriptive test function names starting with `test_`
5. Add docstrings to explain what each test verifies
6. Use the `run_client_command` helper from `test_utils.py` to interact with the NFS client

## Debugging Tests

To debug a failing test:

1. Run the test with `-s` to see print statements:
   ```bash
   pytest tests/unit/test_basic_operations.py -s -v
   ```

2. Enable debug logging:
   ```bash
   python3 tests/run_tests.py unit --log-level=DEBUG
   ```

3. Check the log files in `logs/` for detailed error messages

## Continuous Integration

The test suite can be integrated into a CI/CD pipeline. See `.github/workflows/` for example GitHub Actions workflows.
