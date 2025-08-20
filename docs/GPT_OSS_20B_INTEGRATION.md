# GPT-OSS-20B Local Integration Guide

This document outlines how to integrate the open-source `openai/gpt-oss-20b` model with the data pipeline monitoring framework.

## ⚠️ Important Notice

The `openai/gpt-oss-20b` model is an **open-source model** that requires **local deployment** and cannot be used through OpenAI's standard API. This integration requires significant architectural changes.

## Prerequisites

### Hardware Requirements
- **GPU**: NVIDIA GPU with at least 16GB VRAM
- **RAM**: 32GB+ system RAM recommended
- **Storage**: 40GB+ free space for model weights
- **CUDA**: Compatible CUDA drivers

### Software Requirements
- Python 3.11+
- CUDA Toolkit
- vLLM or Ollama for inference
- Transformers library

## Installation Steps

### 1. Install Dependencies

```bash
# Install vLLM for inference
pip install vllm

# Or install Ollama (alternative)
# curl -fsSL https://ollama.ai/install.sh | sh

# Install additional dependencies
pip install transformers torch torchvision torchaudio
```

### 2. Download Model Weights

```bash
# Using Hugging Face CLI
huggingface-cli download openai/gpt-oss-20b --include "original/*" --local-dir ./models/gpt-oss-20b/

# Or using Git LFS
git clone https://huggingface.co/openai/gpt-oss-20b ./models/gpt-oss-20b/
```

### 3. Set Up Local Inference Server

#### Option A: Using vLLM

```bash
# Start vLLM server
python -m vllm.entrypoints.openai.api_server \
    --model ./models/gpt-oss-20b/ \
    --host 0.0.0.0 \
    --port 8000 \
    --gpu-memory-utilization 0.8
```

#### Option B: Using Ollama

```bash
# Create Ollama modelfile
echo 'FROM ./models/gpt-oss-20b/' > Modelfile

# Build Ollama model
ollama create gpt-oss-20b -f Modelfile

# Run Ollama server
ollama serve
```

## Code Changes Required

### 1. Update Settings Configuration

```python
# config/settings.py modifications
class Settings(BaseSettings):
    # LLM Configuration for local deployment
    llm_provider: str = Field(default="local")
    llm_api_key: str = Field(default="dummy_key_for_local")  # Not used for local
    llm_model: str = Field(default="gpt-oss-20b")
    llm_base_url: str = Field(default="http://localhost:8000/v1")  # vLLM server
    
    # Optional: Local model path
    llm_model_path: Optional[str] = Field(default="./models/gpt-oss-20b/")
```

### 2. Create Custom Model Provider

```python
# agents/local_model_provider.py
from pydantic_ai.models.base import AbstractModel
from pydantic_ai.providers.base import AbstractProvider
import httpx
from typing import Dict, Any

class LocalGPTOSSProvider(AbstractProvider):
    """Custom provider for local gpt-oss-20b deployment."""
    
    def __init__(self, base_url: str = "http://localhost:8000/v1"):
        self.base_url = base_url
        self.client = httpx.AsyncClient(base_url=base_url)
    
    async def chat_completions(self, messages, model_name: str, **kwargs):
        """Make chat completion request to local server."""
        response = await self.client.post(
            "/chat/completions",
            json={
                "model": model_name,
                "messages": messages,
                "temperature": kwargs.get("temperature", 0.7),
                "max_tokens": kwargs.get("max_tokens", 1000),
                **kwargs
            }
        )
        return response.json()

class LocalGPTOSSModel(AbstractModel):
    """Model wrapper for local gpt-oss-20b."""
    
    def __init__(self, model_name: str, provider: LocalGPTOSSProvider):
        self.model_name = model_name
        self.provider = provider
```

### 3. Update Agent Initialization

```python
# agents/orchestrator_agent.py modifications
from .local_model_provider import LocalGPTOSSProvider, LocalGPTOSSModel

def get_llm_model() -> AbstractModel:
    """Get LLM model configuration based on settings."""
    if settings.llm_provider == "local":
        provider = LocalGPTOSSProvider(base_url=settings.llm_base_url)
        return LocalGPTOSSModel(settings.llm_model, provider)
    else:
        # Fallback to OpenAI
        provider = OpenAIProvider(
            base_url=settings.llm_base_url,
            api_key=settings.llm_api_key
        )
        return OpenAIModel(settings.llm_model, provider=provider)
```

### 4. Environment Configuration

```bash
# .env for local deployment
LLM_PROVIDER=local
LLM_API_KEY=dummy_key_for_local
LLM_MODEL=gpt-oss-20b
LLM_BASE_URL=http://localhost:8000/v1
LLM_MODEL_PATH=./models/gpt-oss-20b/
```

## Performance Considerations

### 1. Reasoning Levels
The gpt-oss-20b model supports adjustable reasoning levels:

```python
# In agent prompts, you can specify reasoning level
reasoning_prompt = """
<reasoning_level>medium</reasoning_level>
Your task is to monitor data pipelines...
"""
```

### 2. Memory Optimization
- Use model quantization for lower memory usage
- Implement model sharding for multi-GPU setups
- Consider using FP16 precision

### 3. Batch Processing
- Process multiple monitoring requests in batches
- Implement request queuing for better throughput

## Testing Local Deployment

### 1. Test Local Server

```bash
# Test vLLM server
curl -X POST "http://localhost:8000/v1/chat/completions" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "gpt-oss-20b",
    "messages": [{"role": "user", "content": "Hello!"}],
    "max_tokens": 100
  }'
```

### 2. Run Framework Tests

```bash
# Test with local model
LLM_PROVIDER=local python test-scripts/test_all_platforms_integration.py
```

## Troubleshooting

### Common Issues

1. **CUDA Out of Memory**
   - Reduce `gpu-memory-utilization` parameter
   - Use model quantization
   - Process smaller batches

2. **Model Loading Errors**
   - Verify model weights are complete
   - Check CUDA compatibility
   - Ensure sufficient disk space

3. **Performance Issues**
   - Monitor GPU utilization
   - Adjust batch sizes
   - Consider using FP16 precision

### Monitoring Resources

```bash
# Monitor GPU usage
nvidia-smi -l 1

# Monitor memory usage
htop

# Check vLLM server logs
tail -f vllm_server.log
```

## Production Deployment

### Docker Setup

```dockerfile
# Dockerfile for local deployment
FROM nvidia/cuda:12.1-runtime-ubuntu22.04

RUN apt-get update && apt-get install -y python3 python3-pip git-lfs
RUN pip install vllm transformers torch

COPY models/gpt-oss-20b /app/models/gpt-oss-20b
WORKDIR /app

EXPOSE 8000
CMD ["python", "-m", "vllm.entrypoints.openai.api_server", \
     "--model", "/app/models/gpt-oss-20b", \
     "--host", "0.0.0.0", \
     "--port", "8000"]
```

### Kubernetes Deployment

```yaml
# k8s-gpt-oss-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gpt-oss-20b-server
spec:
  replicas: 1
  selector:
    matchLabels:
      app: gpt-oss-20b
  template:
    metadata:
      labels:
        app: gpt-oss-20b
    spec:
      containers:
      - name: gpt-oss-20b
        image: your-registry/gpt-oss-20b:latest
        ports:
        - containerPort: 8000
        resources:
          requests:
            nvidia.com/gpu: 1
            memory: "32Gi"
          limits:
            nvidia.com/gpu: 1
            memory: "32Gi"
```

## Conclusion

Integrating gpt-oss-20b requires significant infrastructure and code changes. Consider whether the benefits outweigh the complexity compared to using OpenAI's API models like `gpt-4o-mini`.

For production use, the recommended approach is to use OpenAI's API unless you have specific requirements for:
- Complete data privacy (local processing)
- Custom model fine-tuning
- Offline operation capabilities
- Cost optimization for high-volume usage
