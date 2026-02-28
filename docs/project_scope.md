# TravelAssist Project Scope (Step 1)

## Project Name
Travel Assistance Case Management Analytics Platform

## Problem Statement
Travel assistance operations handle time-sensitive cases (medical issues, travel disruptions, emergencies, provider coordination). Delays, escalations, and inconsistent provider response times can increase cost, reduce service quality, and lead to SLA breaches.

This project simulates a travel assistance operation and builds an analytics platform to monitor performance, identify bottlenecks, and support operational decisions.

## Stakeholders
- Operations Manager
- Team Lead / Shift Supervisor
- Provider Network Manager
- Finance / Cost Control Analyst
- QA / Service Quality Analyst

## Core Analytics Questions
1. How many cases are coming in, and when (hour/day/week/month)?
2. Are we meeting first-response SLAs by priority?
3. Which stages create the biggest delays?
4. Which providers or regions are associated with slower response times?
5. Which case types have the highest escalation and cost?
6. What drives reopen rates?
7. How much delay comes from missing documentation?

## Scope (MVP)
Included:
- Structured synthetic data for case operations
- SQL-ready schema and KPI definitions
- Power BI dashboard (later phase)
- Azure data pipeline architecture (later phase)

Not included in MVP:
- Real customer data
- Real PHI/PII
- Production auth and user management
- Real-time streaming

## Case Lifecycle (v1)
1. Case Created
2. Triage Started
3. Provider Contacted
4. Documents Requested (optional)
5. Documents Received (optional)
6. Review / Coordination
7. Approved / Denied / Escalated
8. Arranged / Reimbursed
9. Closed
10. Reopened (optional)

## Success Criteria for Step 1
- Scope document completed
- KPI list defined
- Architecture documented
- Project scaffold created and runnable in VS Code