.PHONY: format lint type-check test all

# Format code
format:
	@echo "Formatting code..."
	isort --profile black src/*.py
	black src/*.py

# Lint code
lint:
	@echo "Linting code..."
	pylint src/*.py

# Type check
type-check:
	@echo "Type checking..."
	mypy src/*.py

# Run tests
test:
	@echo "Running tests..."
	pytest src/

# Execution

# Run scripts
q1-memory:
	@echo "Running q1-memory..."
	python src/q1_memory.py

q1-time:
	@echo "Running q1-time..."
	python src/q1_time.py

# Run all checks
all: format lint type-check test
	@echo "All checks passed!"
