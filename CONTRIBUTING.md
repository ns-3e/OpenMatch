# Contributing to OpenMatch 🚀

Thank you for your interest in contributing to OpenMatch! This document provides guidelines and instructions for contributing to the project.

## 🌟 Code of Conduct

By participating in this project, you agree to abide by our Code of Conduct (see CODE_OF_CONDUCT.md).

## 🔄 Development Workflow

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Make your changes
4. Run tests and linting
5. Commit your changes (`git commit -m 'Add amazing feature'`)
6. Push to your branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

## 🛠️ Development Setup

1. Clone the repository:
```bash
git clone https://github.com/ns-3e/OpenMatch.git
cd openmatch
```

2. Install dependencies:
```bash
pip install -e .[dev]
```

## 📝 Coding Standards

- Use Python 3.7+
- Follow PEP 8 style guide
- Use type hints for all function parameters and return values
- Document all classes and functions using Google-style docstrings
- Keep line length to 88 characters (Black formatter default)
- Sort imports using isort

## ✅ Testing

- Write unit tests for all new functionality
- Maintain test coverage above 90%
- Run tests using pytest:
```bash
pytest tests/
pytest --cov=openmatch tests/
```

## 🔍 Code Quality Checks

Before submitting a PR, ensure all checks pass:

```bash
# Format code
black openmatch/
isort openmatch/

# Run linting
flake8 openmatch/
mypy openmatch/

# Run tests
pytest tests/
```

## 📚 Documentation

- Update documentation for any new features or changes
- Include docstrings for all public functions and classes
- Add examples to the relevant sections in the docs
- Build documentation locally to verify:
```bash
cd docs
make html
```

## 🏗️ Project Structure

The project is organized into several key components:

### Match Engine
```python:openmatch/match/engine.py
startLine: 17
endLine: 19
```

### Trust Framework
```python:openmatch/trust/config.py
startLine: 20
endLine: 22
```

### Merge Processor
```python:openmatch/merge/strategies.py
startLine: 13
endLine: 15
```

## 🔧 Adding New Features

1. **Match Rules**: Add new comparison rules in `match/rules.py`
2. **Trust Rules**: Add new survivorship rules in `trust/rules.py`
3. **Merge Strategies**: Add new merge strategies in `merge/strategies.py`
4. **Connectors**: Add new data source connectors in `connectors/`

## 📦 Dependencies

Core dependencies are listed in requirements.txt. When adding new dependencies:

1. Add to the appropriate section in requirements.txt
2. Document why the dependency is needed
3. Consider impact on install size and performance
4. Prefer well-maintained, widely-used packages

## 🚀 Release Process

1. Update version in `openmatch/__init__.py`
2. Update CHANGELOG.md
3. Create release notes
4. Tag release in git
5. Build and publish to PyPI

## 🐛 Bug Reports

When reporting bugs:

1. Use the bug report template
2. Include Python version and OS
3. Provide minimal reproducible example
4. Include relevant error messages
5. Describe expected vs actual behavior

## 💡 Feature Requests

When proposing features:

1. Use the feature request template
2. Describe the use case
3. Explain expected behavior
4. Consider implementation complexity
5. Discuss alternatives considered

## 🤝 Getting Help

- Open an issue for bugs or feature requests
- Join our Slack community
- Check existing documentation
- Review closed issues for similar problems

## 📄 License

By contributing to OpenMatch, you agree that your contributions will be licensed under the MIT License.


