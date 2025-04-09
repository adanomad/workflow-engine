import workflow_engine as we


async def linear_example():
    output_dir = "./workflow_output"
    resolver = we.resolvers.InMemoryResolver(
        persist_results=True,
        output_dir=output_dir,
    )
    executor = we.WorkflowExecutor(resolver)

    graph = we.WorkflowGraph(
        nodes=[
            generate_text_node := we.Node(
                id="generate_text",
                reference_id=we.functions.builtins.generate_text_file.__name__,
            ),
            process_json_node := we.Node(
                id="process_json",
                reference_id=we.functions.builtins.process_text_to_json.__name__,
            ),
        ],
        edges=[
            we.Edge(
                id=f"{generate_text_node.id}-{process_json_node.id}",
                source=generate_text_node.id,
                target=process_json_node.id,
                sourceHandle="text/plain",
                targetHandle="text/plain",
                target_parameter="input_files",
            ),
        ],
    )
    executor.load_workflow(graph.model_dump(by_alias=True))

    await resolver.initialize()
    run_id, results = await executor.execute()
    await resolver.persist_run_results(run_id)


if __name__ == "__main__":
    import asyncio

    asyncio.run(linear_example())
