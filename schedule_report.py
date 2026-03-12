#!/usr/bin/env python3
"""
Scheduler for Swift Billing Email Reports
Runs every Tuesday and Friday at 2:00 PM IST

Usage:
    1. Run continuously: python schedule_report.py
    2. Or use cron (recommended for servers):
       0 14 * * 2,5 cd /path/to/swift-billing && python email_report.py

For macOS launchd:
    Create a plist file in ~/Library/LaunchAgents/
"""

import schedule
import time
from datetime import datetime
from email_report import send_all_reports

def job():
    """Send both email reports"""
    print(f"\n{'='*60}")
    print(f"Running scheduled reports - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    send_all_reports()

def main():
    print("=" * 60)
    print("Swift Billing - Email Report Scheduler")
    print("=" * 60)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("\nSchedule:")
    print("  - Every Tuesday at 2:00 PM IST")
    print("  - Every Friday at 2:00 PM IST")
    print("\nEmails:")
    print("  Email 1: All parties EXCEPT Mahindra & John Deere")
    print("    To: billing@srlpl.in")
    print("    CC: shyam.wadhwa@srlpl.in, ns@srlpl.in, headops@srlpl.in,")
    print("        finance01@srlpl.in, mis03@srlpl.in, mis@srlpl.in")
    print("\n  Email 2: Only Mahindra & John Deere")
    print("    To: billing01@srlpl.in")
    print("    CC: (same as above)")
    print("\nPress Ctrl+C to stop")
    print("=" * 60)

    # Schedule for Tuesday and Friday at 2:00 PM IST
    schedule.every().tuesday.at("14:00").do(job)
    schedule.every().friday.at("14:00").do(job)

    # Show next scheduled runs
    print("\nNext scheduled runs:")
    for job_item in schedule.get_jobs():
        print(f"  - {job_item}")

    print("\nWaiting for scheduled time...")

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()
