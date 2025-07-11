# Event Hooks Plugin Example

## Overview

This document demonstrates how to implement an event hooks plugin for lakehouse-plumber, similar to Databricks DLT event hooks. This plugin will allow users to react to pipeline events with custom actions.

## Plugin Implementation

### 1. Plugin Structure

```
lhp-event-hooks-plugin/
├── setup.py
├── README.md
├── requirements.txt
├── src/
│   └── lhp_event_hooks/
│       ├── __init__.py
│       ├── plugin.py
│       ├── hooks/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   ├── webhook.py
│       │   ├── databricks_jobs.py
│       │   ├── email.py
│       │   └── cloud_logging.py
│       └── templates/
│           └── webhook_config.yaml
└── tests/
    ├── __init__.py
    ├── test_webhook_hook.py
    └── test_plugin.py
```

### 2. Plugin Definition

```python
# src/lhp_event_hooks/plugin.py
from lhp.plugin.interfaces import IEventHookPlugin, PluginMetadata
from typing import List, Type
from .hooks import WebhookEventHook, DatabricksJobsHook, EmailHook, CloudLoggingHook

class EventHooksPlugin(IEventHookPlugin):
    """Event hooks plugin for lakehouse-plumber."""
    
    def get_metadata(self) -> PluginMetadata:
        return PluginMetadata(
            name="lhp-event-hooks",
            version="1.0.0",
            description="Comprehensive event hooks for pipeline monitoring and automation",
            author="LHP Community",
            requires_lhp_version=">=0.3.0",
            dependencies=[
                "requests>=2.28.0",
                "aiohttp>=3.8.0",
                "databricks-sdk>=0.12.0",
                "boto3>=1.26.0",  # For AWS CloudWatch
                "google-cloud-logging>=3.5.0",  # For GCP
                "azure-monitor>=1.0.0"  # For Azure
            ]
        )
    
    def validate_environment(self) -> bool:
        """Validate that required dependencies are available."""
        try:
            import requests
            import aiohttp
            return True
        except ImportError:
            return False
    
    def get_event_hook_classes(self) -> List[Type]:
        """Return available event hook classes."""
        return [
            WebhookEventHook,
            DatabricksJobsHook,
            EmailHook,
            CloudLoggingHook
        ]
```

### 3. Base Event Hook

```python
# src/lhp_event_hooks/hooks/base.py
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from lhp.plugin.interfaces import IEventHook
from pydantic import BaseModel
import asyncio
import logging

class EventHookConfig(BaseModel):
    """Base configuration for event hooks."""
    enabled: bool = True
    events: List[str] = []  # Empty means all events
    filter_pipelines: Optional[List[str]] = None
    filter_flowgroups: Optional[List[str]] = None
    async_execution: bool = True
    retry_attempts: int = 3
    retry_delay: float = 1.0

class BaseEventHook(IEventHook, ABC):
    """Base class for all event hooks."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = self._parse_config(config)
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        
    @abstractmethod
    def _parse_config(self, config: Dict[str, Any]) -> EventHookConfig:
        """Parse configuration specific to this hook."""
        pass
    
    def get_events(self) -> List[str]:
        """Return list of events this hook handles."""
        return self.config.events if self.config.events else ["*"]
    
    async def handle_event(self, event_name: str, context: Dict[str, Any]) -> None:
        """Handle the event with filtering and error handling."""
        if not self._should_handle_event(event_name, context):
            return
            
        try:
            if self.config.async_execution:
                await self._handle_event_async(event_name, context)
            else:
                await asyncio.get_event_loop().run_in_executor(
                    None, self._handle_event_sync, event_name, context
                )
        except Exception as e:
            self.logger.error(f"Error handling event {event_name}: {e}")
            if self.config.retry_attempts > 0:
                await self._retry_event(event_name, context)
    
    def _should_handle_event(self, event_name: str, context: Dict[str, Any]) -> bool:
        """Check if this event should be handled based on filters."""
        # Check event type filter
        if self.config.events and event_name not in self.config.events:
            return False
            
        # Check pipeline filter
        if self.config.filter_pipelines:
            pipeline = context.get("pipeline")
            if pipeline not in self.config.filter_pipelines:
                return False
                
        # Check flowgroup filter
        if self.config.filter_flowgroups:
            flowgroup = context.get("flowgroup")
            if flowgroup not in self.config.filter_flowgroups:
                return False
                
        return True
    
    async def _retry_event(self, event_name: str, context: Dict[str, Any]) -> None:
        """Retry event handling with exponential backoff."""
        for attempt in range(self.config.retry_attempts):
            try:
                await asyncio.sleep(self.config.retry_delay * (2 ** attempt))
                if self.config.async_execution:
                    await self._handle_event_async(event_name, context)
                else:
                    await asyncio.get_event_loop().run_in_executor(
                        None, self._handle_event_sync, event_name, context
                    )
                return
            except Exception as e:
                self.logger.error(f"Retry {attempt + 1} failed: {e}")
        
        self.logger.error(f"All retries failed for event {event_name}")
    
    @abstractmethod
    async def _handle_event_async(self, event_name: str, context: Dict[str, Any]) -> None:
        """Handle event asynchronously."""
        pass
    
    def _handle_event_sync(self, event_name: str, context: Dict[str, Any]) -> None:
        """Handle event synchronously (default implementation)."""
        raise NotImplementedError("Synchronous handling not implemented")
```

### 4. Webhook Event Hook

```python
# src/lhp_event_hooks/hooks/webhook.py
from typing import Dict, Any, Optional
from .base import BaseEventHook, EventHookConfig
import aiohttp
import json
from datetime import datetime
from pydantic import BaseModel, HttpUrl

class WebhookConfig(EventHookConfig):
    """Configuration for webhook event hook."""
    webhook_url: HttpUrl
    headers: Dict[str, str] = {}
    include_payload: bool = True
    payload_template: Optional[Dict[str, Any]] = None
    timeout: float = 30.0
    verify_ssl: bool = True

class WebhookEventHook(BaseEventHook):
    """Send events to a webhook endpoint."""
    
    def _parse_config(self, config: Dict[str, Any]) -> WebhookConfig:
        return WebhookConfig(**config)
    
    async def _handle_event_async(self, event_name: str, context: Dict[str, Any]) -> None:
        """Send event to webhook asynchronously."""
        payload = self._build_payload(event_name, context)
        
        async with aiohttp.ClientSession() as session:
            async with session.post(
                self.config.webhook_url,
                json=payload,
                headers=self.config.headers,
                timeout=aiohttp.ClientTimeout(total=self.config.timeout),
                ssl=self.config.verify_ssl
            ) as response:
                if response.status >= 400:
                    text = await response.text()
                    raise Exception(f"Webhook returned {response.status}: {text}")
                    
                self.logger.info(f"Successfully sent event {event_name} to webhook")
    
    def _build_payload(self, event_name: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Build webhook payload."""
        base_payload = {
            "event": event_name,
            "timestamp": datetime.utcnow().isoformat(),
            "pipeline": context.get("pipeline"),
            "flowgroup": context.get("flowgroup"),
            "action": context.get("action")
        }
        
        if self.config.payload_template:
            # Apply template
            return self._apply_template(self.config.payload_template, base_payload, context)
        elif self.config.include_payload:
            # Include full context
            base_payload["context"] = context
            
        return base_payload
    
    def _apply_template(self, template: Dict[str, Any], base: Dict[str, Any], 
                       context: Dict[str, Any]) -> Dict[str, Any]:
        """Apply payload template with variable substitution."""
        result = {}
        for key, value in template.items():
            if isinstance(value, str) and value.startswith("${") and value.endswith("}"):
                # Variable substitution
                var_path = value[2:-1]
                result[key] = self._get_nested_value(context, var_path)
            elif isinstance(value, dict):
                # Recursive template application
                result[key] = self._apply_template(value, base, context)
            else:
                result[key] = value
                
        # Merge with base payload
        return {**base, **result}
    
    def _get_nested_value(self, data: Dict[str, Any], path: str) -> Any:
        """Get nested value from dictionary using dot notation."""
        keys = path.split(".")
        value = data
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        return value
```

### 5. Databricks Jobs Hook

```python
# src/lhp_event_hooks/hooks/databricks_jobs.py
from typing import Dict, Any, List, Optional
from .base import BaseEventHook, EventHookConfig
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunNowRequest
import asyncio

class DatabricksJobsConfig(EventHookConfig):
    """Configuration for Databricks jobs event hook."""
    workspace_url: str
    token: str
    job_mappings: Dict[str, int]  # event_pattern -> job_id
    run_name_template: str = "Triggered by {event} in {pipeline}"
    wait_for_completion: bool = False
    timeout_seconds: int = 3600

class DatabricksJobsHook(BaseEventHook):
    """Trigger Databricks jobs based on pipeline events."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.client = WorkspaceClient(
            host=self.config.workspace_url,
            token=self.config.token
        )
    
    def _parse_config(self, config: Dict[str, Any]) -> DatabricksJobsConfig:
        return DatabricksJobsConfig(**config)
    
    async def _handle_event_async(self, event_name: str, context: Dict[str, Any]) -> None:
        """Trigger Databricks job based on event."""
        job_id = self._get_job_for_event(event_name, context)
        if not job_id:
            self.logger.debug(f"No job mapping for event {event_name}")
            return
            
        run_name = self._format_run_name(event_name, context)
        
        # Run in executor to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            None,
            self._trigger_job,
            job_id,
            run_name,
            context
        )
    
    def _get_job_for_event(self, event_name: str, context: Dict[str, Any]) -> Optional[int]:
        """Get job ID for the given event."""
        # Check exact match
        if event_name in self.config.job_mappings:
            return self.config.job_mappings[event_name]
            
        # Check pattern match
        for pattern, job_id in self.config.job_mappings.items():
            if self._matches_pattern(event_name, pattern):
                return job_id
                
        return None
    
    def _matches_pattern(self, event_name: str, pattern: str) -> bool:
        """Check if event name matches pattern (supports wildcards)."""
        import fnmatch
        return fnmatch.fnmatch(event_name, pattern)
    
    def _format_run_name(self, event_name: str, context: Dict[str, Any]) -> str:
        """Format run name using template."""
        return self.config.run_name_template.format(
            event=event_name,
            pipeline=context.get("pipeline", "unknown"),
            flowgroup=context.get("flowgroup", "unknown"),
            timestamp=context.get("timestamp", "")
        )
    
    def _trigger_job(self, job_id: int, run_name: str, context: Dict[str, Any]) -> None:
        """Trigger Databricks job."""
        # Prepare parameters
        parameters = {
            "event_name": context.get("type"),
            "pipeline": context.get("pipeline"),
            "flowgroup": context.get("flowgroup"),
            "metadata": json.dumps(context.get("metadata", {}))
        }
        
        request = RunNowRequest(
            job_id=job_id,
            notebook_params=parameters
        )
        
        run = self.client.jobs.run_now(request)
        self.logger.info(f"Triggered job {job_id} with run_id {run.run_id}")
        
        if self.config.wait_for_completion:
            self._wait_for_job(run.run_id)
    
    def _wait_for_job(self, run_id: int) -> None:
        """Wait for job completion."""
        import time
        start_time = time.time()
        
        while time.time() - start_time < self.config.timeout_seconds:
            run = self.client.jobs.get_run(run_id)
            if run.state.life_cycle_state in ["TERMINATED", "SKIPPED"]:
                if run.state.result_state == "SUCCESS":
                    self.logger.info(f"Job {run_id} completed successfully")
                else:
                    self.logger.error(f"Job {run_id} failed: {run.state.state_message}")
                return
                
            time.sleep(10)
        
        self.logger.error(f"Job {run_id} timed out after {self.config.timeout_seconds}s")
```

### 6. Configuration Example

```yaml
# Example configuration in lhp.yaml
plugins:
  enabled: true
  config:
    event_hooks:
      webhook:
        enabled: true
        webhook_url: "https://api.example.com/pipeline-events"
        headers:
          Authorization: "Bearer ${WEBHOOK_TOKEN}"
          Content-Type: "application/json"
        events:
          - "pipeline.end"
          - "pipeline.error"
        payload_template:
          status: "${event}"
          pipeline_name: "${pipeline}"
          duration_seconds: "${metadata.duration}"
          error_message: "${error.message}"
          
      databricks_jobs:
        enabled: true
        workspace_url: "https://myworkspace.databricks.com"
        token: "${DATABRICKS_TOKEN}"
        job_mappings:
          "pipeline.end": 12345  # Trigger job 12345 on pipeline success
          "pipeline.error": 67890  # Trigger error handler job
          "table.created": 11111  # Trigger downstream processing
        events:
          - "pipeline.end"
          - "pipeline.error"
          - "table.created"
          
      cloud_logging:
        enabled: true
        provider: "aws"  # aws, gcp, or azure
        events: ["*"]  # Log all events
        aws:
          log_group: "/aws/lakehouse-plumber/events"
          region: "us-east-1"
```

### 7. Usage in Pipeline

Once the plugin is installed, event hooks are automatically triggered during pipeline execution:

```yaml
# pipelines/example.yaml
pipeline: my_pipeline
flowgroup: ingestion

# Event hooks are automatically triggered at these points:
# - pipeline.start: When pipeline begins
# - flowgroup.start: When this flowgroup starts
# - action.start: Before each action
# - action.end: After each action succeeds
# - flowgroup.end: When flowgroup completes
# - pipeline.end: When entire pipeline completes

actions:
  - name: load_data
    type: load
    # This will trigger:
    # - action.start (action=load_data)
    # - action.end (action=load_data)
    source:
      type: cloudfiles
      path: "/data/input"
    target: v_raw_data
    
  - name: write_table
    type: write
    # This will trigger:
    # - action.start (action=write_table)
    # - table.created (table=my_table) - special event
    # - action.end (action=write_table)
    source: v_raw_data
    write_target:
      type: streaming_table
      database: "my_catalog.bronze"
      table: "my_table"
```

### 8. Advanced Features

#### Custom Event Types

```python
# In your pipeline code, emit custom events
from lhp.plugin.events import EventEmitter, Event, EventType

# Define custom event type
class CustomEventType(str, Enum):
    DATA_QUALITY_CHECK = "data.quality_check"
    SCHEMA_EVOLUTION = "schema.evolution"

# Emit custom event
emitter.emit(Event(
    type=CustomEventType.DATA_QUALITY_CHECK,
    pipeline="my_pipeline",
    metadata={
        "table": "customers",
        "failed_rows": 152,
        "total_rows": 10000,
        "failure_rate": 0.0152
    }
))
```

#### Conditional Event Handling

```python
class ConditionalWebhookHook(WebhookEventHook):
    """Only send webhook if certain conditions are met."""
    
    def _should_handle_event(self, event_name: str, context: Dict[str, Any]) -> bool:
        if not super()._should_handle_event(event_name, context):
            return False
            
        # Only send error events if they're critical
        if event_name == "pipeline.error":
            error = context.get("error")
            if error and hasattr(error, "severity"):
                return error.severity == "CRITICAL"
                
        # Only send completion events for production pipelines
        if event_name == "pipeline.end":
            pipeline = context.get("pipeline", "")
            return pipeline.startswith("prod_")
            
        return True
```

### 9. Testing

```python
# tests/test_webhook_hook.py
import pytest
from unittest.mock import patch, AsyncMock
from lhp_event_hooks.hooks.webhook import WebhookEventHook

@pytest.mark.asyncio
async def test_webhook_event_handling():
    config = {
        "webhook_url": "https://test.example.com/webhook",
        "events": ["pipeline.end"],
        "headers": {"X-API-Key": "test-key"}
    }
    
    hook = WebhookEventHook(config)
    
    context = {
        "pipeline": "test_pipeline",
        "flowgroup": "test_group",
        "metadata": {"duration": 120}
    }
    
    with patch("aiohttp.ClientSession.post") as mock_post:
        mock_response = AsyncMock()
        mock_response.status = 200
        mock_post.return_value.__aenter__.return_value = mock_response
        
        await hook.handle_event("pipeline.end", context)
        
        # Verify webhook was called
        mock_post.assert_called_once()
        call_args = mock_post.call_args
        
        assert call_args[0][0] == "https://test.example.com/webhook"
        assert call_args[1]["headers"]["X-API-Key"] == "test-key"
        assert "pipeline" in call_args[1]["json"]
```

## Benefits

1. **Automation**: Automatically trigger downstream processes
2. **Monitoring**: Real-time pipeline status updates
3. **Alerting**: Immediate notification of failures
4. **Integration**: Connect with existing tools and workflows
5. **Flexibility**: Custom event types and handlers
6. **Reliability**: Built-in retry and error handling

## Conclusion

This event hooks plugin provides a powerful way to extend lakehouse-plumber with custom reactions to pipeline events, enabling sophisticated automation and monitoring capabilities similar to Databricks DLT event hooks.