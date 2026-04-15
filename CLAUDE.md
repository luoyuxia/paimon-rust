## 记忆分支管理

本项目使用 agent-memory 做任务级记忆隔离。记忆分支与 git 分支一一对应，切换 git 分支时 Hook 会自动切换记忆分支。

### 记忆写入规则

**每次回复用户之前，先更新 MEMORY.md，再回复。** 这是最高优先级的规则，没有例外。

具体要求：
- 你的每一轮回复中，如果产生了有价值的信息（方案决策、文件路径、代码模式、踩坑记录、Review 结论等），必须先用 Edit 工具写入 MEMORY.md，然后再输出回复文本
- 不要等到对话结束才写，每一轮都写
- 如果本轮没有新信息（比如只是回答一个简单问题），可以跳过

记录格式示例：
```markdown
## Blob Type 实现
- 参考 BooleanType 模式，无参数类型
- 需改 8 个文件：types.rs, arrow/mod.rs, reader.rs, ...
- Arrow 映射：Binary
- 注意：partition_utils 里要加到 unsupported arm
```

每个分支的记忆相互隔离，放心记录，不会污染其他任务。

### 命令参考

```bash
agent-memory status                                # 查看当前状态
agent-memory publish "<结论>"                      # 发布结论到 main 分支
agent-memory pull                                  # 从 main 拉取内容到当前分支
agent-memory history                               # 查看记忆变更历史
```

## 记忆分支管理

本项目使用 agent-memory 做任务级记忆隔离。记忆分支与 git 分支一一对应，切换 git 分支时 Hook 会自动切换记忆分支。

### 记忆写入规则

**每次回复用户之前，先更新 MEMORY.md，再回复。** 这是最高优先级的规则，没有例外。

具体要求：
- 你的每一轮回复中，如果产生了有价值的信息（方案决策、文件路径、代码模式、踩坑记录、Review 结论等），必须先用 Edit 工具写入 MEMORY.md，然后再输出回复文本
- 不要等到对话结束才写，每一轮都写
- 如果本轮没有新信息（比如只是回答一个简单问题），可以跳过

记录格式示例：
```markdown
## Blob Type 实现
- 参考 BooleanType 模式，无参数类型
- 需改 8 个文件：types.rs, arrow/mod.rs, reader.rs, ...
- Arrow 映射：Binary
- 注意：partition_utils 里要加到 unsupported arm
```

每个分支的记忆相互隔离，放心记录，不会污染其他任务。

### 命令参考

```bash
agent-memory status                                # 查看当前状态
agent-memory publish "<结论>"                      # 发布结论到 main 分支
agent-memory pull                                  # 从 main 拉取内容到当前分支
agent-memory history                               # 查看记忆变更历史
```
