#!/usr/bin/env python3
"""
Scheduler for Swift Billing Email Reports
Runs every Tuesday and Friday at 9:00 AM

Usage:
    1. Run continuously: python schedule_report.py
    2. Or use cron (recommended for servers):
       0 9 * * 2,5 cd /path/to/swift-billing && python email_report.py

For macOS launchd (runs even when logged out):
    See setup instructions below
"""

import schedule
import time
from datetime import datetime
from email_report import send_email_report

def job():
    """Send the email report"""
    print(f"\n{'='*50}")
    print(f"Running scheduled report - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")
    send_email_report()

def main():
    print("Swift Billing - Email Report Scheduler")
    print("=" * 50)
    print("Scheduled to run every Tuesday and Friday at 9:00 AM")
    print("Press Ctrl+C to stop\n")

    # Schedule for Tuesday and Friday at 9:00 AM
    schedule.every().tuesday.at("09:00").do(job)
    schedule.every().friday.at("09:00").do(job)

    # Show next scheduled runs
    print("Next scheduled runs:")
    for job_item in schedule.get_jobs():
        print(f"  - {job_item}")

    print("\nWaiting for scheduled time...")

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()
