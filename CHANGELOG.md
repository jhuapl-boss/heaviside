# Heaviside Changelog
## 2.2.5
 * upped setuptools to version 75.3.0 in requirements to avoid previous version's vulnerabilities
 * upped setuptools-scm to 8.1.0
 
## 2.2.4
 * Added support for Python 3.11
 * Switched from `setup.py` to `pyproject.toml`

## 2.2.3
 * Update funcparserlib version to ensure compatibility with Python 3.8 and above.
 * Dropped support for Python 3.7

## 2.2.2
 * Catch duplicate state names within a map's iterator

## 2.2.1
 * Fixed issues with Map state with while blocks and resolving ARNs

## 2.2.0
 * Added support for Map state

## 2.1.0
 * Fixed bug in packaging `aws_service.json` definition file
 * Fix bug in `heaviside.ast.StateVisitor` implementation
 * Added support for updating existing Step Functions
 * Added support for specifying parameters for Pass states

## 2.0
 * New features
    - Added support for `goto` control flow construct
    - Added support for AWS API tasks and passing parameters to them
 * Improvements
    - Cleaned up unicode handling so that any string can contain unicode characters
    - Created [Library API](docs/LibraryAPI.md) documentation that expands upon the older Activities documentation and contains all of the public API that Heaviside exposes

## 1.1
 * New features
    - Added new `activities.fanout_nonblocking` that allows the StepFunction to use a `Wait()` state instead of waiting within the Activity or Lambda
    - Integration tests that cover as many code paths as possible
 * Improvements
    - Unit tests
    - Documentation
 * Bug fixes
    - Nested `while` loop
    - `activities.fanout` error handling

## 1.0
 * Initial public release of Heaviside
 * Added AST into the compiler pipeline
 * Added `activities.fanout`
 * Added support for `switch`, `parallel`, `while` control flow constructs
 * Refactored activities code into mixins for better reuse
 * Multiple bug fixes

## 0.8
 * Initial release of Heaviside
