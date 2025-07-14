# Auto Loader Comprehensive Templates

This directory contains comprehensive YAML templates for Databricks Auto Loader with all available configuration options based on the [Auto Loader documentation](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options).

> **⚠️ Important:**  
> These reference templates are **not used in the project**. They are provided for reference only.

> **⚠️ Important:**  
> These Databricks configuration files **may not be up to date**. They are provided for reference only.  
> For the most current information, always consult the [official Auto Loader documentation](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options).


## Overview

These templates provide complete configurations for:
- **All supported file formats** with format-specific options
- **All cloud providers** (AWS, Azure, GCP) with cloud-specific configurations
- **Detailed comments** explaining each option and its purpose
- **Default values** and examples for all parameters

## File Format Templates

Each file format template includes:
- Common Auto Loader options
- Directory listing options
- Format-specific options
- Cloud-specific configurations (AWS example)
- Comprehensive comments explaining each option

### Supported File Formats

| File Format | Template File | Description |
|-------------|---------------|-------------|
| **CSV** | `autoloader_csv_comprehensive.yaml` | CSV files with all parsing options |
| **JSON** | `autoloader_json_comprehensive.yaml` | JSON files with parsing flexibility |
| **Parquet** | `autoloader_parquet_comprehensive.yaml` | Parquet files with optimization options |
| **Avro** | `autoloader_avro_comprehensive.yaml` | Avro files with schema evolution |
| **XML** | `autoloader_xml_comprehensive.yaml` | XML files with tag parsing |
| **ORC** | `autoloader_orc_comprehensive.yaml` | ORC files with schema merging |
| **Text** | `autoloader_text_comprehensive.yaml` | Plain text files |
| **Binary** | `autoloader_binaryfile_comprehensive.yaml` | Binary files (images, PDFs, etc.) |

## Cloud Provider Templates

Cloud-specific templates include authentication methods, notification services, and cloud-native features.

### Supported Cloud Providers

| Cloud Provider | Template File | Key Features |
|----------------|---------------|--------------|
| **AWS** | `autoloader_aws_comprehensive.yaml` | S3, SNS, SQS, IAM roles, service credentials |
| **Azure** | `autoloader_azure_comprehensive.yaml` | Blob Storage, Event Grid, Queue Storage, service principals |
| **GCP** | `autoloader_gcp_comprehensive.yaml` | Cloud Storage, Pub/Sub, service accounts |

## Common Configuration Options

All templates include these major configuration sections:

### 1. Common Auto Loader Options
- File format specification
- Schema evolution modes
- File processing limits
- Backfill configurations
- Clean source options

### 2. Directory Listing Options
- Recursive file lookup
- Path filtering with glob patterns
- Time-based file filtering
- File modification constraints

### 3. File Notification Options
- Cloud-native event notifications
- Queue configurations
- Notification service setup
- Managed file events

### 4. Authentication Options
- Databricks service credentials (recommended)
- Cloud-specific authentication methods
- IAM roles and service principals
- Access keys and secrets

### 5. Performance Tuning
- Concurrent request limits
- Timeout configurations
- Retry policies
- Batch size optimization

## Usage Examples

### Using File Format Templates

```yaml
# Example: Load CSV files with comprehensive options
name: my_csv_pipeline
version: "1.0"

parameters:
  table_name: "customer_data"
  source_path: "/path/to/csv/files"
  target_database: "bronze_layer"
  target_table: "customers"

# Inherit from CSV template
extends: autoloader_csv_comprehensive.yaml
```

### Using Cloud Provider Templates

```yaml
# Example: Load from AWS S3 with notifications
name: aws_s3_pipeline
version: "1.0"

parameters:
  table_name: "transaction_data"
  source_path: "s3://my-bucket/transactions/"
  target_database: "bronze_layer"
  target_table: "transactions"
  file_format: "json"

# Inherit from AWS template
extends: autoloader_aws_comprehensive.yaml
```

## Key Configuration Sections Explained

### Schema Evolution
```yaml
# Control how new columns are handled
cloudFiles.schemaEvolutionMode: "addNewColumns"  # Allow new columns
# cloudFiles.schemaEvolutionMode: "none"         # Strict schema
# cloudFiles.schemaEvolutionMode: "rescue"       # Rescue mismatched data
```

### Error Handling
```yaml
# Handle corrupt or mismatched records
cloudFiles.rescueDataColumn: "_rescued_data"     # Store bad records
cloudFiles.mode: "PERMISSIVE"                    # Don't fail on errors
# cloudFiles.mode: "FAILFAST"                    # Fail on first error
# cloudFiles.mode: "DROPMALFORMED"               # Drop bad records
```

### Performance Optimization
```yaml
# Control processing volume
cloudFiles.maxFilesPerTrigger: 50                # Limit files per batch
cloudFiles.maxBytesPerTrigger: "1g"             # Limit data per batch
cloudFiles.maxConcurrentRequests: 50            # Parallel request limit
```

### File Cleanup
```yaml
# Automatic file management
cloudFiles.cleanSource: "MOVE"                   # Move processed files
cloudFiles.cleanSource.moveDestination: "/archive"
cloudFiles.cleanSource.retentionDuration: "30 days"
```

## Authentication Best Practices

### 1. Databricks Service Credentials (Recommended)
```yaml
databricks.serviceCredential: "my-service-credential"
```

### 2. AWS IAM Roles
```yaml
cloudFiles.roleArn: "arn:aws:iam::123456789012:role/MyRole"
cloudFiles.roleExternalId: "external-id"
```

### 3. Azure Service Principals
```yaml
cloudFiles.clientId: "app-id"
cloudFiles.clientSecret: "secret"
cloudFiles.tenantId: "tenant-id"
```

### 4. GCP Service Accounts
```yaml
cloudFiles.clientEmail: "service-account@project.iam.gserviceaccount.com"
cloudFiles.privateKey: "-----BEGIN PRIVATE KEY-----..."
```

## Notification Services

### AWS (S3 + SNS + SQS)
```yaml
cloudFiles.useNotifications: true
cloudFiles.region: "us-east-1"
# Auto Loader will set up SNS topic and SQS queue automatically
```

### Azure (Blob + Event Grid + Queue Storage)
```yaml
cloudFiles.useNotifications: true
cloudFiles.resourceGroup: "my-resource-group"
cloudFiles.subscriptionId: "subscription-id"
```

### GCP (Cloud Storage + Pub/Sub)
```yaml
cloudFiles.useNotifications: true
cloudFiles.projectId: "my-gcp-project"
```

## Template Customization

### 1. Copy Template
Choose the appropriate template for your use case and copy it to your project.

### 2. Update Parameters
Modify the parameters section with your specific values:
```yaml
parameters:
  - name: table_name
    required: true
    description: "Your table name"
  - name: source_path
    required: true  
    description: "Your source path"
```

### 3. Enable Required Options
Uncomment and configure the options you need:
```yaml
# cloudFiles.useNotifications: true    # Uncomment to enable
cloudFiles.useNotifications: true      # Now enabled
```

### 4. Add Format-Specific Options
Each format template includes all available options for that format. Enable the ones relevant to your data:

**CSV Options:**
```yaml
cloudFiles.header: true                 # Files have headers
cloudFiles.delimiter: ","              # Field separator
cloudFiles.multiline: false            # Single-line records
```

**JSON Options:**
```yaml
cloudFiles.multiline: false            # Single-line JSON objects
cloudFiles.allowComments: false        # JSON comments allowed
cloudFiles.dateFormat: "yyyy-MM-dd"    # Date parsing format
```

## Troubleshooting

### Common Issues

1. **Permission Errors**
   - Verify authentication configuration
   - Check IAM roles/service principal permissions
   - Ensure access to source and notification services

2. **Schema Evolution Errors**  
   - Review `schemaEvolutionMode` setting
   - Check `rescuedDataColumn` for problematic data
   - Validate `schemaHints` format

3. **Performance Issues**
   - Adjust `maxFilesPerTrigger` and `maxBytesPerTrigger`
   - Tune `maxConcurrentRequests`
   - Consider file notification mode for better performance

4. **Notification Setup Failures**
   - Verify cloud provider permissions
   - Check resource group/project configurations
   - Ensure notification services are available in the region

## References

- [Databricks Auto Loader Documentation](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options)
- [Auto Loader Best Practices](https://docs.databricks.com/ingestion/auto-loader/production.html)
- [Schema Evolution Guide](https://docs.databricks.com/ingestion/auto-loader/schema.html)

## Contributing

When adding new templates or updating existing ones:

1. Include comprehensive comments for all options
2. Provide default values and examples
3. Test templates with actual data sources
4. Update this README with new features
5. Follow the established naming conventions

---

> **Note:** 
>These templates are based on Databricks Runtime 16.1+ and include the latest Auto Loader features. Some options may not be available in older runtime versions. 