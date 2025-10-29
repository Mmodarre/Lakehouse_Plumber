# Template Quoting Investigation Report

## Summary

Investigation into why Kafka secrets weren't being converted to `dbutils.secrets.get()` calls in generated code.

## Root Cause (Already Fixed in kafka.py.j2)

The issue was that Jinja templates were not escaping double quotes in string values, which caused:

1. **Invalid Python Syntax**: When a config value contains embedded double quotes (like Kafka JAAS config), wrapping it in double quotes creates invalid Python:
   ```python
   .option("key", "value with "embedded" quotes")  # ✗ BROKEN
   ```

2. **SecretCodeGenerator Failure**: The `SecretCodeGenerator` uses regex to find valid Python string literals. When the string is malformed, the regex fragments it into multiple pieces, missing the secret placeholders inside.

3. **Result**: Secret placeholders remain unconverted in the final code.

## Current Status

**✓ FIXED**: `src/lhp/templates/load/kafka.py.j2` (line 12)
- Has escaping: `"{{ value|replace('"', '\\"') }}"`
- Generates valid Python with escaped quotes
- Secrets are properly converted to `dbutils.secrets.get()` calls
- Generated file compiles successfully

**Note**: The file attached to the conversation was outdated. After force regeneration, the current file is correct.

## Other Templates With Same Vulnerability

### 1. **cloudfiles.py.j2** - VULNERABLE
**Lines 26 and 38:**
```jinja2
.option("{{ key }}", "{{ value }}")
```

**Risk**: CloudFiles options that contain quotes (like custom schemas, filters, or complex configurations) will:
- Generate invalid Python syntax
- Fail SecretCodeGenerator processing if they contain secret placeholders
- Cause syntax errors at runtime

**Example scenario**: If a cloudFiles option contains SQL-like syntax with quotes:
```yaml
options:
  cloudFiles.schemaLocation: "/path/with/quotes"
  someFilter: "field = 'value'"  # Would break
```

---

### 2. **custom_datasource.py.j2** - VULNERABLE
**Lines 20 and 35 (streaming and batch modes):**
```jinja2
.option("{{ key }}", "{{ value }}")
```

**Risk**: Custom data source options that contain quotes will:
- Generate invalid Python syntax
- Fail if secret placeholders are used in quoted strings
- Break on complex configuration values

**Example scenario**: Custom data source with authentication config:
```yaml
options:
  auth_config: '{"username": "${secret:scope/user}"}'  # Would break
```

---

### 3. **jdbc.py.j2** - VULNERABLE
**Lines 6-11:**
```jinja2
.option("url", "{{ jdbc_url }}") \
.option("user", "{{ jdbc_user }}") \
.option("password", "{{ jdbc_password }}") \
.option("driver", "{{ jdbc_driver }}") \
{% if jdbc_query %}        .option("query", """{{ jdbc_query }}""") \
{% elif jdbc_table %}        .option("dbtable", "{{ jdbc_table }}") \
```

**Risk**: JDBC connection strings and passwords that contain quotes will:
- Generate invalid Python syntax
- **CRITICAL**: JDBC URLs often contain semicolons and quotes for parameters
- Password fields with special characters will break
- Secret substitution will fail

**Example scenarios**:
```yaml
# JDBC URL with parameters (common pattern)
url: "jdbc:sqlserver://host:1433;database=mydb;encrypt=true"  # Would break if has quotes

# Password with quotes
password: "${secret:db/password}"  # If password value contains quotes, breaks

# Complex JDBC queries
query: 'SELECT * FROM table WHERE field = "value"'  # Would break
```

---

## Why This Wasn't Caught Before

1. **Most common use cases don't include quotes** in configuration values
2. **The Kafka JAAS config is unusual** in requiring embedded double quotes for username/password
3. **Black formatter fails** but doesn't prevent code generation
4. **Runtime errors only occur** when the specific configuration is used

## Solution Pattern (Already Applied to kafka.py.j2)

Replace:
```jinja2
.option("{{ key }}", "{{ value }}")
```

With:
```jinja2
.option("{{ key }}", "{{ value|replace('"', '\\"') }}")
```

This escapes any double quotes in the value, making the generated Python valid.

## Additional Findings

### Templates That Are SAFE (No Dynamic String Values):
- `delta.py.j2` - Uses table references, not dynamic string options
- `sql.py.j2` - Uses triple-quoted SQL, no .option() calls
- `python.py.j2` - Uses `tojson` filter for parameters
- All transform templates - Don't use .option() with dynamic strings
- All write templates - Use `tojson` for complex values

### Why Black Formatting Failed
The generated code with unescaped quotes cannot be parsed:
```
WARNING: Black formatting failed: Cannot parse for target version Python 3.8: 
  28:130: .option("kafka.sasl.jaas.config", "...username="__SECRET_...
```

## Recommendations

1. **Apply the same escaping fix** to the 3 vulnerable templates:
   - `cloudfiles.py.j2`
   - `custom_datasource.py.j2`
   - `jdbc.py.j2`

2. **Consider using a custom Jinja filter** for consistent escaping across all templates

3. **Add validation tests** that generate code with:
   - Values containing double quotes
   - Values containing single quotes
   - Values with secret placeholders and quotes
   - Complex JDBC URLs with parameters

4. **Document the limitation** if not fixed: "Configuration values containing double quotes must escape them manually"

## Test Cases Needed

```python
# Test 1: Value with double quotes
options:
  key: 'value with "quotes" inside'

# Test 2: Value with secret and quotes
options:
  auth: 'token="${secret:scope/token}"'

# Test 3: JDBC URL with parameters
jdbc:
  url: 'jdbc:sqlserver://host:1433;database=mydb;encrypt=true;quote="value"'

# Test 4: Complex JAAS config (already tested with Kafka)
options:
  jaas.config: '...username="${secret:user}"...'
```

---

**Investigation Date**: 2025-10-29
**Generated File Status**: Working correctly after force regeneration
**Templates Needing Fix**: 3 (cloudfiles, custom_datasource, jdbc)

