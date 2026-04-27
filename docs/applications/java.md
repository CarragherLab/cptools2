# Java

> **Source:** <https://www.wiki.ed.ac.uk/display/ResearchServices/Java>

Java is provided through environment modules on Eddie. Use the module version rather than the OS-provided Java.

```bash
# List available Java versions (Research Services modules)
module available java

# List all available versions including community modules
module available | grep 'java\|jdk'
```

## Memory Recommendations

Java should not be run on the Eddie login nodes, which have strict virtual memory limits. When requesting resources for a submitted job or interactive session, we recommend **not requesting virtual memory** — this gives you the default (unlimited) virtual memory.

```bash
# Interactive session with unlimited virtual memory
qlogin
```

For more information on specifying memory, see [Memory Specification](../memory-specification.md).

> **Note:** The Java modules set the environment variable `MALLOC_ARENA_MAX=1` to control memory usage.
