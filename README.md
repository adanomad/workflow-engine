# Workflow Engine

Workflow Engine is a modular workflow orchestration system designed for running and chaining node-based tasks. It currently supports a file-based data model for persisting node outputs and passing data between nodes based on MIME types.

## Getting Started

```sh
pip install ... # TODO
```

See the `examples` folder for example workflows.

## Features

- **Graph-Based Workflow Execution:**
  Execute workflows defined as directed graphs. The engine uses topological sorting for dependency resolution.

- **Modular Node Execution:**
  Each node processes inputs, executes a function, and produces outputs. Data is passed between nodes based on MIME types.

- **File-Based Data Persistence:**
  Node outputs are wrapped in a `FileExecutionData` object, saved to a document store (using Supabase), and retrieved by matching MIME types.

- **Flexible Resolver Architecture:**
  The engine decouples workflow logic from storage and function retrieval using a `BaseResolver` interface, with a concrete `SupabaseResolver` implementation.

- **Robust Error Handling and Logging:**
  Built-in error propagation and logging help to diagnose and resolve issues during workflow execution.

- **Future Enhancements:**
  Planned improvements include support for:
  - A more flexible data model (e.g., ephemeral and in-memory data)
  - Iterative workflows and sub-workflows (allowing controlled cycles)
  - Enhanced concurrency, parallel support

### Key Modules

- **`workflow.py`**
  Contains the `WorkflowExecutor` class, which:
  - Loads and validates workflows as directed acyclic graphs (DAGs)
  - Executes nodes in topological order
  - Handles input gathering, function invocation, and saving of results

- **`types.py`**
  Contains the Pydantic and dataclass models for:
  - Defining the workflow structure (`Node`, `Edge`, `WorkflowGraph`)
  - Representing file-based data (`File`, `FileExecutionData`)
  - Utility functions like `calc_file_size`

## Development

```sh
# with poetry (preferred)
poetry install

# with pip
pip install -r requirements.txt
pip install -e .
```

## Tests
(mock and integration test)
```sh
poetry run pytest
```

## TODO

Allow nodes to be run for multiple iterations
Support parallel workflows
Finish in_memory implementation
