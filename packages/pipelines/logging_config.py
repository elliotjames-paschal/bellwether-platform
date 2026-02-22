"""
Bellwether Pipeline Logging System

Provides centralized logging with:
- Console output (INFO and above)
- Full log files (all levels)
- Error-only log files
- Email alerts on ERROR/CRITICAL
- Log rotation (keep last 50 runs)

Usage:
    from logging_config import setup_logging, get_logger

    # In orchestrator (once per run):
    setup_logging(run_name="daily")

    # In any script:
    logger = get_logger("script_name")
    logger.info("Starting...")
    logger.debug("Detailed info")
    logger.warning("Something unexpected")
    logger.error("Something failed")
"""

import logging
import os
import json
import smtplib
import glob
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from pathlib import Path
from typing import Optional, List
import atexit

# =============================================================================
# CONFIGURATION
# =============================================================================

from config import BASE_DIR
LOGS_DIR = BASE_DIR / "logs"
RUNS_DIR = LOGS_DIR / "pipeline_runs"
EMAIL_CONFIG_FILE = LOGS_DIR / "email_config.json"

# How many log files to keep
MAX_LOG_FILES = 50

# Global state
_current_run_name: Optional[str] = None
_current_log_file: Optional[Path] = None
_error_log_file: Optional[Path] = None
_error_buffer: List[str] = []
_run_start_time: Optional[datetime] = None
_initialized: bool = False


# =============================================================================
# CUSTOM FORMATTERS
# =============================================================================

class ConsoleFormatter(logging.Formatter):
    """Clean, readable console output with symbols."""

    SYMBOLS = {
        logging.DEBUG: "  ",
        logging.INFO: "",
        logging.WARNING: "  \u26a0",  # Warning symbol
        logging.ERROR: "  \u2717 ERROR:",  # X mark
        logging.CRITICAL: "  \u2717 CRITICAL:",
    }

    def format(self, record):
        symbol = self.SYMBOLS.get(record.levelno, "")
        timestamp = datetime.now().strftime("%H:%M:%S")

        # Indent continuation lines
        message = record.getMessage()
        if symbol.startswith("  "):
            return f"[{timestamp}]{symbol} {message}"
        else:
            return f"[{timestamp}] {message}"


class FileFormatter(logging.Formatter):
    """Detailed format for log files."""

    def format(self, record):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        level = record.levelname.ljust(8)
        source = record.name.ljust(20)
        message = record.getMessage()
        return f"{timestamp} | {level} | {source} | {message}"


# =============================================================================
# ERROR BUFFER HANDLER (for email)
# =============================================================================

class ErrorBufferHandler(logging.Handler):
    """Buffers ERROR and CRITICAL messages for email summary."""

    def __init__(self):
        super().__init__(level=logging.ERROR)
        self.errors: List[dict] = []

    def emit(self, record):
        self.errors.append({
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'level': record.levelname,
            'source': record.name,
            'message': record.getMessage()
        })

    def get_errors(self) -> List[dict]:
        return self.errors

    def clear(self):
        self.errors = []


# Global error handler
_error_handler: Optional[ErrorBufferHandler] = None


# =============================================================================
# SETUP FUNCTIONS
# =============================================================================

def setup_logging(run_name: str = "daily", console_level: int = logging.INFO) -> logging.Logger:
    """
    Initialize logging for a pipeline run.

    Args:
        run_name: Name for this run (e.g., "daily", "full_refresh")
        console_level: Minimum level for console output (default INFO)

    Returns:
        Root logger for the pipeline
    """
    global _current_run_name, _current_log_file, _error_log_file
    global _error_handler, _run_start_time, _initialized

    # Create directories
    LOGS_DIR.mkdir(exist_ok=True)
    RUNS_DIR.mkdir(exist_ok=True)

    # Generate log file names
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    _current_run_name = run_name
    _run_start_time = datetime.now()

    _current_log_file = RUNS_DIR / f"{timestamp}_{run_name}.log"
    _error_log_file = RUNS_DIR / f"{timestamp}_{run_name}_errors.log"

    # Create symlink to latest log
    latest_link = LOGS_DIR / "latest.log"
    if latest_link.exists() or latest_link.is_symlink():
        latest_link.unlink()
    latest_link.symlink_to(_current_log_file)

    # Get root logger
    root_logger = logging.getLogger("bellwether")
    root_logger.setLevel(logging.DEBUG)

    # Clear any existing handlers
    root_logger.handlers.clear()

    # Console handler (INFO and above)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(ConsoleFormatter())
    root_logger.addHandler(console_handler)

    # Full log file handler (all levels)
    file_handler = logging.FileHandler(_current_log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(FileFormatter())
    root_logger.addHandler(file_handler)

    # Error-only file handler
    error_file_handler = logging.FileHandler(_error_log_file, encoding='utf-8')
    error_file_handler.setLevel(logging.ERROR)
    error_file_handler.setFormatter(FileFormatter())
    root_logger.addHandler(error_file_handler)

    # Error buffer for email
    _error_handler = ErrorBufferHandler()
    root_logger.addHandler(_error_handler)

    # Register cleanup on exit
    if not _initialized:
        atexit.register(_on_exit)
        _initialized = True

    # Rotate old logs
    rotate_logs()

    return root_logger


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger for a specific script/module.

    Args:
        name: Name of the script (e.g., "discover_markets", "classify_categories")

    Returns:
        Logger instance
    """
    return logging.getLogger(f"bellwether.{name}")


# =============================================================================
# LOG ROTATION
# =============================================================================

def rotate_logs():
    """Remove old log files, keeping only the most recent MAX_LOG_FILES."""
    # Find all log files (excluding error logs to count main logs only)
    log_files = glob.glob(str(RUNS_DIR / "*_*.log"))
    log_files = [f for f in log_files if not f.endswith("_errors.log")]

    if len(log_files) <= MAX_LOG_FILES:
        return

    # Sort by modification time (oldest first)
    log_files.sort(key=lambda x: os.path.getmtime(x))

    # Delete oldest files
    to_delete = log_files[:-MAX_LOG_FILES]
    for filepath in to_delete:
        try:
            os.remove(filepath)
            # Also remove corresponding error log
            error_log = filepath.replace(".log", "_errors.log")
            if os.path.exists(error_log):
                os.remove(error_log)
        except OSError:
            pass


# =============================================================================
# EMAIL ALERTS
# =============================================================================

def load_email_config() -> Optional[dict]:
    """Load email configuration from JSON file."""
    if not EMAIL_CONFIG_FILE.exists():
        return None

    try:
        with open(EMAIL_CONFIG_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return None


def send_error_email(errors: List[dict], run_name: str, start_time: datetime):
    """
    Send email summary of errors.

    Args:
        errors: List of error dicts with timestamp, level, source, message
        run_name: Name of the pipeline run
        start_time: When the run started
    """
    config = load_email_config()
    if not config:
        logger = get_logger("email")
        logger.debug("No email config found, skipping email alert")
        return

    try:
        # Build email content
        date_str = start_time.strftime("%Y-%m-%d")
        subject = f"\U0001f534 Bellwether Pipeline Error - {date_str} {run_name} run"

        body_lines = [
            f"Bellwether Pipeline Error Report",
            f"=" * 50,
            f"",
            f"Run: {run_name}",
            f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}",
            f"Total Errors: {len(errors)}",
            f"",
            f"=" * 50,
            f"ERROR DETAILS",
            f"=" * 50,
            f"",
        ]

        for i, error in enumerate(errors, 1):
            body_lines.extend([
                f"Error {i}:",
                f"  Time: {error['timestamp']}",
                f"  Level: {error['level']}",
                f"  Source: {error['source']}",
                f"  Message: {error['message']}",
                f"",
            ])

        body_lines.extend([
            f"=" * 50,
            f"Log file: {_current_log_file}",
            f"Error log: {_error_log_file}",
        ])

        body = "\n".join(body_lines)

        # Create email message
        msg = MIMEMultipart()
        msg['From'] = config.get('from_email', config.get('smtp_user'))
        msg['To'] = ", ".join(config.get('recipients', []))
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))

        # Send via SMTP
        smtp_host = config.get('smtp_host', 'smtp.stanford.edu')
        smtp_port = config.get('smtp_port', 25)
        smtp_user = config.get('smtp_user')
        smtp_password = config.get('smtp_password')

        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            # Try STARTTLS (required by many servers including Stanford)
            try:
                server.starttls()
            except smtplib.SMTPNotSupportedError:
                pass  # Server doesn't support TLS, continue anyway
            # Only authenticate if credentials provided
            if smtp_user and smtp_password:
                server.login(smtp_user, smtp_password)
            server.send_message(msg)

        logger = get_logger("email")
        logger.info(f"Error alert email sent to {len(config.get('recipients', []))} recipient(s)")

    except Exception as e:
        logger = get_logger("email")
        logger.warning(f"Failed to send error email: {e}")


def _on_exit():
    """Called on program exit - send error email if needed."""
    global _error_handler, _run_start_time, _current_run_name

    if _error_handler and _error_handler.errors and _run_start_time:
        send_error_email(_error_handler.errors, _current_run_name or "unknown", _run_start_time)


def flush_email():
    """Manually trigger email send (call at end of pipeline)."""
    global _error_handler, _run_start_time, _current_run_name

    if _error_handler and _error_handler.errors and _run_start_time:
        send_error_email(_error_handler.errors, _current_run_name or "unknown", _run_start_time)
        _error_handler.clear()


# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

def log_header(title: str, char: str = "=", width: int = 70):
    """Log a formatted header."""
    logger = get_logger("orchestrator")
    logger.info(char * width)
    logger.info(title.center(width))
    logger.info(char * width)


def log_phase(phase_num: int, title: str):
    """Log a phase header."""
    logger = get_logger("orchestrator")
    logger.info("")
    logger.info(f"PHASE {phase_num}: {title}")
    logger.info("-" * 50)


def log_step_start(step_name: str):
    """Log the start of a pipeline step."""
    logger = get_logger("orchestrator")
    logger.info(f"Starting: {step_name}...")


def log_step_done(step_name: str, duration_seconds: float, success: bool = True):
    """Log the completion of a pipeline step."""
    logger = get_logger("orchestrator")
    status = "Done" if success else "FAILED"
    logger.info(f"{status}: {step_name} ({duration_seconds:.1f}s)")


def log_summary(results: dict, total_duration: float):
    """
    Log pipeline summary.

    Args:
        results: Dict with 'success', 'failed', 'skipped' counts
        total_duration: Total run time in seconds
    """
    logger = get_logger("orchestrator")

    logger.info("")
    logger.info("=" * 70)
    logger.info("PIPELINE COMPLETE".center(70))
    logger.info("=" * 70)

    minutes = int(total_duration // 60)
    seconds = int(total_duration % 60)
    logger.info(f"Duration: {minutes} minutes {seconds} seconds")

    success = results.get('success', 0)
    failed = results.get('failed', 0)
    skipped = results.get('skipped', 0)
    logger.info(f"Results: {success} success, {failed} failed, {skipped} skipped")

    if _error_handler:
        error_count = len(_error_handler.errors)
        logger.info(f"Errors logged: {error_count}")
        if error_count > 0:
            logger.info("Email alert sent: Yes")

    logger.info("=" * 70)


def get_error_count() -> int:
    """Get the number of errors logged so far."""
    if _error_handler:
        return len(_error_handler.errors)
    return 0


# =============================================================================
# CONVENIENCE: SIMPLE LOG FUNCTION (for scripts not yet migrated)
# =============================================================================

def log(msg: str, level: str = "info", source: str = "script"):
    """
    Simple log function for backward compatibility.

    Args:
        msg: Message to log
        level: "debug", "info", "warning", "error", "critical"
        source: Source name for the log entry
    """
    logger = get_logger(source)
    level_map = {
        "debug": logger.debug,
        "info": logger.info,
        "warning": logger.warning,
        "error": logger.error,
        "critical": logger.critical,
    }
    log_func = level_map.get(level.lower(), logger.info)
    log_func(msg)
