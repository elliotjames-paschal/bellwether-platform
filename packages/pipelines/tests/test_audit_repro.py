import sys
sys.path.insert(0, "/home/groups/andyhall/bellwether-platform/packages/pipelines")
from audit.audit_daily_summary import *
from audit.audit_validator import DataValidator
from audit.audit_anomaly import AnomalyDetector
import pandas as pd
df = pd.read_csv("/home/groups/andyhall/bellwether-platform/data/combined_political_markets_with_electoral_details_UPDATED.csv", low_memory=False)
new_markets_summary, new_markets_samples = get_new_markets_summary(df, days=1)
validator = DataValidator()
validation = validator.run_all_checks(source="pre_publish")
detector = AnomalyDetector()
anomalies = detector.run_all_checks().to_dict()
panel_a_audit = get_panel_a_audit(df)
panel_b_audit = get_panel_b_audit(df)
body = format_email_body(df, new_markets_summary, new_markets_samples, validation, anomalies, panel_a_audit, panel_b_audit)
print("SUCCESS")
