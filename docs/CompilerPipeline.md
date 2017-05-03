# Heaviside Compiler Pipeline

This document describes the different stages of the Heaviside compiler.

## Table of Contents:

 * [Overview](#Overview)
 * [Lexer](#Lexer)
 * [Parser](#Parser)
 * [Abstract Syntax Tree](#Abstract-Syntax-Tree)
   - [Transformations](#Transformations)
 * [State Machine Generation](#State-Machine-Generation)

## Overview
The design of the lexer and parser and heavily influenced by the tutorials from
the [funcparserlib](https://github.com/vlasovskikh/funcparserlib) library used
by Heaviside.

Currently `compile` (`heaviside/__init__.py`) reads in the source file,
tokenizes it, and then passes the results to the parser. The parser is then
responsible for the rest of the pipeline.

## Lexer
The lexer (`heaviside/lexer.py`) is just a wrapper around the Python tokenizer.
This helps gives the Heaviside DSL a Python like style with minimal effort. The
results of the Python tokenizer are wrapped in a custom class.

## Parser
The parser (`heaviside/parser.py`) implements the Heaviside parsing logic
using the [funcparserlib](https://github.com/vlasovskikh/funcparserlib) library.
The parsed tokens are wrapped in Abstract Syntax Tree (AST) nodes that are built
into a structure representing the source file.

## Abstract Syntax Tree
The Abstract Syntax Tree (AST) (`heaviside/ast.py`) is used to represent the
structure of the source file in memory. Each AST node contains the token from
the source file that it represents. This allows raising an error anywhere
during the compilation process and being able to attach the context of the
error (location in the source and the text of the line with the error).

### Transformations
After tokens are parsed into AST nodes several transformations are applied to
the AST structure. The transformations are at the bottom of `heaviside/ast.py`.

The most important transformation is linking the different states together.
When parsed into AST nodes, they don't contain information about the next node
to transition to (which has to be explicitly specified). Linking the nodes
together adds information about the next state to transition to. It also moves
the blocks of states for the different Choice State branches into the correct
location in the AST structure (they exist at the same level as the Choice State).

Another transformation is passing each of the Task ARNs to a callback function
so that they can be modified (if desired). This often means resolving an
Activity or Lambda name to a full ARN.

The last transformation is a check on state names. It make sure each name is
valid and that there are not any duplicate names.

## State Machine Generation
The final step is to convert the AST to the State Machine JSON representation.
In `heaviside/sfn.py` are Python classes that represent each of the different
State Machine structures. They are sub-classes of dict, so they can be
serialized to JSON with minimal work. Each class takes the equivalent AST node
as argument and creates the needed JSON entries.

Because this is the final step, minimal checking is done to the data. There are
a few checks that happen during this step, but the AST structure is expected to
be mostly / completely compliant with the States Language specification.

