# **Data Contract Playbook: Wind Farm Data Asset**

This document serves as the official playbook for defining, implementing, and controlling the wind\_farm\_data asset using Data Contracts within OpenMetadata 1.8.

## **1\. Overview**

The primary goal of this playbook is to ensure data reliability, quality, and trustworthiness by establishing a formal, automated agreement between the data producers and consumers of the wind\_farm\_data asset.

This process is managed through an Automation Agent (a Python script) that interacts with the OpenMetadata API to enforce the contract terms.

Scope: This playbook applies specifically to the wind\_farm\_data asset. The canonical schema is defined in the wind\_farm\_schema.json file, which is the single source of truth for the data structure.

## **2\. Roles & Responsibilities**

| Role | Responsibility |
| :---- | :---- |
| Data Producer | The team or system responsible for generating the source wind farm data. Accountable for fixing data issues at the source that cause contract breaches. |
| Data Steward | The individual or team responsible for defining contract terms, managing the contract lifecycle (Draft, Active, Deprecated), and mediating between producers and consumers during a breach. |
| Data Consumer | Any team or application that relies on the wind\_farm\_data asset. Must adhere to the active contract and participate in change management reviews. |
| Automation Agent | A programmatic script integrated into a CI/CD pipeline, responsible for creating, updating, and checking the contract status against the data asset in OpenMetadata. |

## **3\. The Control Process**

The process is divided into four phases, managed by the Automation Agent and overseen by the Data Steward.

### **Phase 1: Definition and Contract Creation**

*Since OMD 1.8 lacks a UI for contracts, this phase is entirely API-driven.*

1. Translate Rules into Contract Terms: The Data Steward defines the contract based on three pillars:  
   * Schema Contract: The structure is taken directly from the wind\_farm\_schema.json artifact.  
   * Quality Contract: Define critical data quality tests (e.g., columnValuesToBeUnique, columnValueMinToBe).  
   * Freshness Contract: Define the maximum acceptable data latency (e.g., 24 hours).  
2. Develop the Automation Agent Script: A Python script is created using the OpenMetadata SDK to programmatically define and create the Data Contract.  
3. Initial Deployment (Draft State): The agent runs for the first time, creating the contract in OpenMetadata with the status set to Draft. This allows for review without enforcement.  
4. Stakeholder Review: The Data Steward notifies all stakeholders to review the Draft contract. Feedback is incorporated via updates to the agent script.

### **Phase 2: Activation and Enforcement**

5. Activate the Contract: Once approved, the agent script is updated to change the contract status from Draft to Active. The contract is now live and enforced.  
6. Integrate Agent into CI/CD Pipeline: The agent script is integrated into the data ingestion pipeline. It should be configured to run *after* each data load. The pipeline's success or failure now depends on the contract's status.  
7. Automated Monitoring: On every run, the agent performs the following checks via the OpenMetadata API:  
   * Schema Check: Compares the table's current schema against the contract.  
   * Quality Check: Triggers the linked Data Quality tests.  
   * Freshness Check: Verifies the lastUpdatedAt timestamp against the freshness rule.

### **Phase 3: Breach Management & Resolution**

8. Breach Detection & Pipeline Failure: If any check fails, the contract is breached. The agent script must exit with an error code, causing the CI/CD pipeline to fail. This prevents bad data from propagating downstream.  
9. Automated Alerting: Upon failure, an automated alert is sent to a dedicated channel (e.g., Slack \#data-contracts-alerts) with details of the breach and a link to the asset in OpenMetadata.  
10. Root Cause Analysis & Resolution:  
    * The on-call Data Producer investigates the breach using the OpenMetadata UI (lineage, profiler, etc.).  
    * Once the issue is fixed at the source, the data ingestion pipeline is re-run. A successful run signifies the breach is resolved.

### **Phase 4: Change Management**

11. Proposing a Change: No changes can be made directly to an Active contract. To propose a change, the Data Steward must update the agent script to create a new version of the contract, which starts in the Draft state.  
12. Review and Activation of New Version: The new Draft version is reviewed by stakeholders. Once approved, the agent sets its status to Active. OpenMetadata automatically marks the previous version as Deprecated, ensuring a clear and auditable history.