import pytest

from workflow_engine import (
    BooleanValue,
    Edge,
    InputEdge,
    IntegerValue,
    OutputEdge,
    Workflow,
)
from workflow_engine.contexts import InMemoryContext
from workflow_engine.execution import TopologicalExecutionAlgorithm
from workflow_engine.nodes import AddNode, ConstantIntegerNode, IfElseNode


@pytest.fixture
def add_one_workflow() -> Workflow:
    """Create a workflow that adds one to a number."""

    return Workflow(
        nodes=[
            one := ConstantIntegerNode.from_value(id="one", value=1),
            add_one := AddNode(id="add_one"),
        ],
        edges=[
            Edge.from_nodes(
                source=one,
                source_key="value",
                target=add_one,
                target_key="b",
            ),
        ],
        input_edges=[
            InputEdge.from_node(
                input_key="start",
                target=add_one,
                target_key="a",
            ),
        ],
        output_edges=[
            OutputEdge.from_node(
                source=add_one,
                source_key="sum",
                output_key="result",
            ),
        ],
    )


@pytest.fixture
def subtract_one_workflow() -> Workflow:
    """Create a workflow that subtracts one from a number."""

    return Workflow(
        nodes=[
            negative_one := ConstantIntegerNode.from_value(id="negative_one", value=-1),
            subtract_one := AddNode(id="subtract_one"),
        ],
        edges=[
            Edge.from_nodes(
                source=negative_one,
                source_key="value",
                target=subtract_one,
                target_key="b",
            ),
        ],
        input_edges=[
            InputEdge.from_node(
                input_key="start",
                target=subtract_one,
                target_key="a",
            ),
        ],
        output_edges=[
            OutputEdge.from_node(
                source=subtract_one,
                source_key="sum",
                output_key="result",
            ),
        ],
    )


@pytest.mark.asyncio
async def test_conditional_workflow(
    add_one_workflow: Workflow,
    subtract_one_workflow: Workflow,
):
    """Test that the workflow outputs start+1 when condition is True, and
    start-1 when condition is False."""
    context = InMemoryContext()
    algorithm = TopologicalExecutionAlgorithm()

    start_value = 42

    workflow = Workflow(
        nodes=[
            conditional := IfElseNode.from_workflows(
                id="conditional",
                if_true=add_one_workflow,
                if_false=subtract_one_workflow,
            ),
        ],
        edges=[],
        input_edges=[
            InputEdge.from_node(
                input_key="start",
                target=conditional,
                target_key="start",
            ),
            InputEdge.from_node(
                input_key="condition",
                target=conditional,
                target_key="condition",
            ),
        ],
        output_edges=[
            OutputEdge.from_node(
                source=conditional,
                source_key="result",
                output_key="result",
            ),
        ],
    )

    errors, output = await algorithm.execute(
        context=context,
        workflow=workflow,
        input={
            "start": IntegerValue(start_value),
            "condition": BooleanValue(False),
        },
    )
    assert not errors.any(), errors
    assert output == {"result": start_value - 1}

    errors, output = await algorithm.execute(
        context=context,
        workflow=workflow,
        input={
            "start": IntegerValue(start_value),
            "condition": BooleanValue(True),
        },
    )
    assert not errors.any(), errors
    assert output == {"result": start_value + 1}


@pytest.mark.asyncio
async def test_conditional_workflow_twice_series(
    add_one_workflow: Workflow,
    subtract_one_workflow: Workflow,
):
    """Test that the workflow behaves correctly when condition is called twice
    in series, once with True and once with False."""
    context = InMemoryContext()
    algorithm = TopologicalExecutionAlgorithm()

    start_value = 42

    workflow = Workflow(
        nodes=[
            conditional_1 := IfElseNode.from_workflows(
                id="conditional_1",
                if_true=add_one_workflow,
                if_false=subtract_one_workflow,
            ),
            conditional_2 := IfElseNode.from_workflows(
                id="conditional_2",
                if_true=add_one_workflow,
                if_false=subtract_one_workflow,
            ),
        ],
        edges=[
            Edge.from_nodes(
                source=conditional_1,
                source_key="result",
                target=conditional_2,
                target_key="start",
            ),
        ],
        input_edges=[
            InputEdge.from_node(
                input_key="start",
                target=conditional_1,
                target_key="start",
            ),
            InputEdge.from_node(
                input_key="condition_1",
                target=conditional_1,
                target_key="condition",
            ),
            InputEdge.from_node(
                input_key="condition_2",
                target=conditional_2,
                target_key="condition",
            ),
        ],
        output_edges=[
            OutputEdge.from_node(
                source=conditional_2,
                source_key="result",
                output_key="result",
            ),
        ],
    )

    errors, output = await algorithm.execute(
        context=context,
        workflow=workflow,
        input={
            "start": IntegerValue(start_value),
            "condition_1": BooleanValue(True),
            "condition_2": BooleanValue(False),
        },
    )
    assert not errors.any(), errors
    assert output == {"result": start_value}

    errors, output = await algorithm.execute(
        context=context,
        workflow=workflow,
        input={
            "start": IntegerValue(start_value),
            "condition_1": BooleanValue(False),
            "condition_2": BooleanValue(True),
        },
    )
    assert not errors.any(), errors
    assert output == {"result": start_value}

    errors, output = await algorithm.execute(
        context=context,
        workflow=workflow,
        input={
            "start": IntegerValue(start_value),
            "condition_1": BooleanValue(True),
            "condition_2": BooleanValue(True),
        },
    )
    assert not errors.any(), errors
    assert output == {"result": start_value + 2}

    errors, output = await algorithm.execute(
        context=context,
        workflow=workflow,
        input={
            "start": IntegerValue(start_value),
            "condition_1": BooleanValue(False),
            "condition_2": BooleanValue(False),
        },
    )
    assert not errors.any(), errors
    assert output == {"result": start_value - 2}
