# my_workflow_engine/functions/builtins.py
from ..functionRegistry import registry

# map and reduce operations should be configured on the implementation level

@registry.register(name="calculator", description="Perform mathematical calculations")
async def calculator(expression: str) -> str:
    return str(eval(expression))


# ...
