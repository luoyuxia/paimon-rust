---
name: memory-create
description: Create a new memory branch and switch to it
allowed-tools: Bash
argument-hint: [branch-name]
---

Create memory branch `$ARGUMENTS` and switch to it:

```!
agent-memory branch create $ARGUMENTS && agent-memory branch switch $ARGUMENTS
```
