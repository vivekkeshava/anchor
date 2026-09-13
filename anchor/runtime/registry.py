"""Agent registration. A run stores an agent *name*; the worker resolves it to a callable."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict

from anchor.errors import UnknownAgentError

AgentFn = Callable[[Any, "object"], Awaitable[Any]]


class AgentRegistry:
    def __init__(self) -> None:
        self._agents: Dict[str, AgentFn] = {}

    def register(self, name: str, fn: AgentFn) -> None:
        self._agents[name] = fn

    def get(self, name: str) -> AgentFn:
        try:
            return self._agents[name]
        except KeyError:
            raise UnknownAgentError(
                f"no agent registered as {name!r}; this worker knows: "
                f"{sorted(self._agents) or '(none)'}"
            ) from None

    def names(self) -> list[str]:
        return sorted(self._agents)


def agent(name: str, registry: AgentRegistry) -> Callable[[AgentFn], AgentFn]:
    def decorate(fn: AgentFn) -> AgentFn:
        registry.register(name, fn)
        return fn

    return decorate
