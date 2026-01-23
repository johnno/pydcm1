# Development Guide

## Setting up development environment

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Run tests: `python test_protocol.py`

## Building and Publishing

### Building the package

```bash
python setup.py sdist bdist_wheel
```

### Publishing to PyPI

```bash
twine upload dist/*
```

## Testing

Run the test suite:

```bash
python -m pytest
```

Run manual tests with your DCM1:

```bash
python main.py -h <your-dcm1-ip> -a
```
