---
name: memory-done
description: Finish debugging - publish conclusion to main and clean up temp branch
allowed-tools: Bash
argument-hint: [conclusion text]
---

Current branch:

```!
agent-memory branch current
```

Publish the conclusion to main, switch back, and delete the temp branch.

Steps:
1. Run `agent-memory publish "$ARGUMENTS"` to publish the conclusion
2. Run `agent-memory branch list` to find the non-temp branch to switch back to
3. Run `agent-memory branch switch <target>` to switch back
4. Run `agent-memory branch delete <temp-branch>` to clean up

Execute these steps now.
