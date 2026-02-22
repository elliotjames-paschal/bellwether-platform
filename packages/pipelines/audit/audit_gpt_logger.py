"""
================================================================================
GPT AUDIT LOGGER
================================================================================

Logs all GPT API calls for audit purposes:
- Input markets and questions
- Model and parameters
- Full responses
- Parsed results
- Token usage and latency

Storage: JSONL format (one JSON object per line) for efficient appending.
"""

import json
import time
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from functools import wraps

from .audit_config import GPT_LOGS_DIR


class GPTAuditLogger:
    """
    Logs GPT API calls for audit purposes.

    Usage:
        logger = GPTAuditLogger("categories")

        # Log a call manually
        logger.log_call(
            stage=1,
            batch_index=0,
            input_markets=[{"market_id": "123", "question": "..."}],
            model="gpt-4o",
            prompt="...",
            response=response_obj,
            parsed_results=[{"category": "1."}],
            latency_ms=1500
        )
    """

    def __init__(self, script_name: str, log_dir: Path = None):
        """
        Initialize logger for a specific script.

        Args:
            script_name: Short name like "categories", "electoral", "winners"
            log_dir: Directory for log files (defaults to GPT_LOGS_DIR)
        """
        self.script_name = script_name
        self.log_dir = log_dir or GPT_LOGS_DIR
        self.log_dir.mkdir(parents=True, exist_ok=True)

        date_str = datetime.now().strftime("%Y-%m-%d")
        self.log_file = self.log_dir / f"{date_str}_{script_name}.jsonl"

        self.call_count = 0
        self.total_tokens = 0
        self.total_errors = 0

    def log_call(
        self,
        stage: int,
        batch_index: int,
        input_markets: List[Dict],
        model: str,
        prompt: str,
        response: Any,
        parsed_results: List[Dict],
        latency_ms: int,
        temperature: float = 0,
        retries: int = 0,
        error: str = None
    ):
        """
        Log a single GPT API call.

        Args:
            stage: Classification stage (1, 2, 3)
            batch_index: Batch number within this run
            input_markets: List of dicts with market_id, question
            model: Model name (gpt-4o, gpt-4o-search-preview)
            prompt: System prompt used
            response: Raw OpenAI response object
            parsed_results: Extracted results from response
            latency_ms: Response time in milliseconds
            temperature: Model temperature
            retries: Number of retry attempts
            error: Error message if failed
        """
        # Extract token usage from response
        tokens_input = 0
        tokens_output = 0
        response_text = None

        if response and hasattr(response, 'usage'):
            tokens_input = response.usage.prompt_tokens
            tokens_output = response.usage.completion_tokens

        if response and hasattr(response, 'choices') and response.choices:
            response_text = response.choices[0].message.content

        entry = {
            "timestamp": datetime.now().isoformat(),
            "script": self.script_name,
            "stage": stage,
            "batch_index": batch_index,
            "batch_size": len(input_markets),
            "input_markets": [
                {
                    "market_id": str(m.get("market_id", "")),
                    "question": str(m.get("question", ""))[:200]  # Truncate long questions
                }
                for m in input_markets
            ],
            "model": model,
            "temperature": temperature,
            "prompt_hash": hashlib.md5(prompt.encode()).hexdigest()[:8],
            "response_raw": response_text,
            "parsed_results": parsed_results,
            "tokens_input": tokens_input,
            "tokens_output": tokens_output,
            "latency_ms": latency_ms,
            "retries": retries,
            "error": error
        }

        # Append to log file
        with open(self.log_file, 'a') as f:
            f.write(json.dumps(entry) + '\n')

        # Update stats
        self.call_count += 1
        self.total_tokens += tokens_input + tokens_output
        if error:
            self.total_errors += 1

    def log_error(self, stage: int, batch_index: int, error: str,
                  input_markets: List[Dict] = None):
        """Log an API error."""
        self.log_call(
            stage=stage,
            batch_index=batch_index,
            input_markets=input_markets or [],
            model="",
            prompt="",
            response=None,
            parsed_results=[],
            latency_ms=0,
            error=error
        )

    def get_stats(self) -> Dict:
        """Get logging statistics."""
        return {
            "script": self.script_name,
            "log_file": str(self.log_file),
            "call_count": self.call_count,
            "total_tokens": self.total_tokens,
            "total_errors": self.total_errors
        }


class GPTCallTimer:
    """Context manager to time GPT API calls."""

    def __init__(self):
        self.start_time = None
        self.latency_ms = 0

    def __enter__(self):
        self.start_time = time.time()
        return self

    def __exit__(self, *args):
        self.latency_ms = int((time.time() - self.start_time) * 1000)


def create_logger(script_name: str) -> GPTAuditLogger:
    """Factory function to create a logger for a script."""
    return GPTAuditLogger(script_name)


# Convenience loggers for common scripts
def get_categories_logger() -> GPTAuditLogger:
    return GPTAuditLogger("categories")


def get_electoral_logger() -> GPTAuditLogger:
    return GPTAuditLogger("electoral")


def get_winners_logger() -> GPTAuditLogger:
    return GPTAuditLogger("winners")


def get_reclassify_logger() -> GPTAuditLogger:
    return GPTAuditLogger("reclassify")
