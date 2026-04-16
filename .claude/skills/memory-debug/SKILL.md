---
name: memory-debug
description: Create a temp debug branch for issue investigation
allowed-tools: Bash
argument-hint: [issue-name]
---

Create temporary debug branch `temp-debug-$ARGUMENTS` and switch to it:

```!
agent-memory branch create temp-debug-$ARGUMENTS && agent-memory branch switch temp-debug-$ARGUMENTS
```

You are now in an isolated debug context. When debugging is done, use `/memory-done` to publish conclusions and clean up.
