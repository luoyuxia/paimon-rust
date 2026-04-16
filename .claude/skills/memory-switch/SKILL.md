---
name: memory-switch
description: Switch to a different memory branch
allowed-tools: Bash
argument-hint: [branch-name]
---

Switch memory branch to `$ARGUMENTS`:

```!
agent-memory branch switch $ARGUMENTS
```

Confirm the result:

```!
agent-memory branch current
```
