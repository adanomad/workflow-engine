# My Workflow Engine

My Workflow Engine is a modular and extensible workflow orchestration system designed for running and chaining node-based tasks. It currently supports a file-based data model for persisting node outputs and passing data between nodes based on MIME types. The engine is built to be extended for more flexible data handling—including ephemeral, JSON, and in-memory data—in future releases.

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
  - Enhanced concurrency and dynamic input resolution

## Package Structure
workflow_engine/
├── __init__.py
├── workflow.py         # Core workflow execution logic
├── resolvers.py        # Resolver interfaces and implementations (e.g., SupabaseResolver)
├── types.py            # Data models and type definitions (Nodes, Edges, Files, etc.)
└── registry.py         # (Optional) Function registry for node functions


### Key Modules

- **`workflow.py`**  
  Contains the `WorkflowExecutor` class, which:
  - Loads and validates workflows as directed acyclic graphs (DAGs)
  - Executes nodes in topological order
  - Handles input gathering, function invocation, and saving of results

- **`resolvers.py`**  
  Defines the `BaseResolver` abstract class and the `SupabaseResolver` implementation. Responsibilities include:
  - Fetching node output data based on MIME types
  - Retrieving function implementations for nodes
  - Fetching node-specific configuration parameters
  - Saving node execution results to a document store

- **`types.py`**  
  Contains the Pydantic and dataclass models for:
  - Defining the workflow structure (`Node`, `Edge`, `WorkflowGraph`)
  - Representing file-based data (`File`, `FileExecutionData`)
  - Utility functions like `calc_file_size`

## Installation

Clone the repository and install the package in editable mode:

```bash
git clone https://github.com/yourusername/my_workflow_engine.git
cd my_workflow_engine
pip install -e .



