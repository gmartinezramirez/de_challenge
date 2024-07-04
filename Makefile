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
q1_memory:
	@echo "Running q1-memory..."
	python src/q1_memory.py

q1_time:
	@echo "Running q1-time..."
	python src/q1_time.py

q2_memory:
	@echo "Running q2-memory..."
	python src/q2_memory.py

q3_memory:
	@echo "Running q3-memory..."
	python src/q3_memory.py


# Run all checks
all: format lint type-check test
	@echo "All checks passed!"
