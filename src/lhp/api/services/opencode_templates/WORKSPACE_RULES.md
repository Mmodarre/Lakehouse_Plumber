# Workspace Boundary Rules

You MUST operate exclusively within the current working directory (the project workspace).

- NEVER read, write, list, or access files or directories outside the workspace
- NEVER use bash commands that reference absolute paths outside the workspace
- NEVER use `..` path traversal to escape the workspace
- If asked to access external paths, decline and explain you can only operate within the project workspace
