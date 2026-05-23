---
name: project-baseaihook-design
description: Architecture decisions for BaseAIHook refactor — AgentRunRequest, BaseToolset, framework-agnostic wiring in common-ai provider
metadata:
  type: project
---

Multi-session design discussion for `providers/common/ai` framework-agnostic agent abstraction.

**Why:** Make `AgentOperator` backend-agnostic — adding a new LLM framework (e.g. Strands) should require only a new hook, not operator changes. Strands PR blocked on this.

## Decisions reached

### Contract: `execute_agent(AgentRunRequest) -> AgentRunResult`
- Single abstract method on `BaseAIHook` replacing `create_agent()` + `run_agent()`
- `AgentRunRequest` is a parameter-object dataclass (prompt, output_type, instructions, toolsets, usage_limits, message_history, enable_tool_logging, durable_context, agent_params)
- `AgentRunResult` gains `durable_stats: DurableStats | None`
- `DurableContext` dataclass (dag_id, task_id, run_id, map_index) — framework-neutral identity for durable caching

### AgentOperator
- Zero pydantic-ai imports (not even TYPE_CHECKING)
- `toolsets: list[Any]`, `usage_limits: Any`
- Builds `AgentRunRequest`, calls `hook.execute_agent(request)` — done
- Durable stats logged from `run_result.durable_stats`
- `_build_agent()`, `_build_durable_toolsets()` deleted

### BaseToolset (pending final design decision — see alternatives below)
- `get_tools() -> list[Callable]` approach: infers name/description from `__name__`/docstring
- Alternative: `get_tool_descriptors() -> list[ToolDescriptor]` with explicit name/description/schema

### Callable-level wrappers in BaseAIHook
- `_adapt_toolsets(toolsets, enable_logging, durable_storage, durable_counter)` — shared pipeline
- `_logged_callable(fn, logger)` — replaces LoggingToolset
- `_cached_callable(fn, storage, counter)` — replaces CachingToolset (pydantic-ai WrapperToolset)
- `_register_tool(fn) -> Any` — template method; default no-op; each hook overrides

### Per-hook registration
- `PydanticAIHook._register_tool(fn)` → returns fn (pydantic-ai infers from annotations)
- `StrandsAIHook._register_tool(fn)` → `strands.tool(fn)`
- `PydanticAIHook._adapt_toolsets()` can override entirely if needed (uses same callable pipeline)

### Model-level durable (pydantic-ai only)
- `CachingModel` stays — wraps pydantic-ai model calls, no framework-agnostic equivalent
- `supports_durable = False` for Strands

### Files to delete after refactor
- `durable/caching_toolset.py` — replaced by `_cached_callable()`
- `toolsets/logging.py` (LoggingToolset) — replaced by `_logged_callable()`

### SQLToolset refactor
- Currently imports pydantic-ai: `ModelRetry`, `ToolDefinition`, `AbstractToolset`
- After: extends `BaseToolset`, plain Python methods, `ValueError` replaces `ModelRetry`

## Open question
**BaseToolset metadata strategy** — `get_tools()` callables (inferred) vs `ToolDescriptor` objects (explicit). See research notes below.

**Why:** This affects portability across frameworks that have different inference strategies.

## Open source reference patterns
- LangChain: explicit `name`, `description`, `args_schema` (Pydantic) on `BaseTool`
- LlamaIndex: `ToolMetadata(name, description, fn_schema)` dataclass; `to_tool_list()`
- Semantic Kernel: `KernelFunctionMetadata` with full parameter list; plugin-based
- Haystack: `@tool` decorator auto-generates JSON schema from type hints (lowest boilerplate)
- Spring AI: `ToolCallback` interface — `getName()`, `getDescription()`, `getInputSchema()`, `call()`
