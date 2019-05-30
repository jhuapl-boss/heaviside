# Heaviside Changelog

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
