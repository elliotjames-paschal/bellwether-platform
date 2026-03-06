import sys
sys.path.insert(0, "/home/groups/andyhall/bellwether-platform/packages/pipelines")
from audit.audit_daily_summary import generate_and_send_summary
from audit.audit_validator import DataValidator
from audit.audit_anomaly import AnomalyDetector
import pandas as pd

# Test 1: Validator (the actual crash)
print("Testing validator...")
validator = DataValidator()
validation = validator.run_all_checks(source="pre_publish")
print(f"  Validation: {validation['status']}")

# Test 2: Anomaly detector
print("Testing anomaly detector...")
detector = AnomalyDetector()
anomalies = detector.run_all_checks().to_dict()
print(f"  Anomalies: {anomalies['anomalies_detected']}")

# Test 3: Full summary generation
print("Testing full summary...")
generate_and_send_summary()
print("SUCCESS")
