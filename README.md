---
editor_options: 
  markdown: 
    wrap: 72
---

# DEGO Course Project - Team 9

# NovaCred Credit Application Governance Analysis

## Team Members

-   Data Engineer: [Noah Kreyenkamp, 71459]
-   Data Scientist: [Finley Marynek, 64723]
-   Governance Officer: [Margarida Passos da Cunha, 71119]
-   Product Lead: [Nina Berenice Wecera, 72603]

## Project Description - Executive Summary

This project applies data governance, fairness analysis, and privacy
engineering principles to NovaCred's credit application dataset (\~500
records). The analysis is structured across three notebooks, each
addressing a distinct governance dimension.

**Notebook 01 - Data Quality** audits the raw dataset across four
quality dimensions: completeness, consistency, validity, and accuracy.
Key issues identified include inconsistent gender encoding (6 distinct
representations across 114 records), four conflicting date formats,
duplicate application records, and duplicate SSNs across different
applicants. A cleaning pipeline was developed to systematically
remediate these issues, resulting in a standardized analytical dataset.

**Notebook 02 - Bias & Fairness Analysis** evaluates disparities in loan
approval outcomes across gender, age, and geography. A statistically
significant gender gap was identified (Female: 50.6% vs. Male: 65.9%; DI
ratio = 0.768, p = 0.0008), which persists across income brackets,
suggesting the disparity is not fully explained by financial
differences. Approval rates also rise systematically with age (\~40% for
under-30s to \~64% for over-60s; p = 0.01), with the gender gap most
pronounced among younger applicants. ZIP code approval rates vary widely
but are based on small sample sizes, making structural bias
inconclusive. Effect sizes remain moderate (Cramér's V ≈ 0.15
throughout), indicating that no single attribute fully explains approval
outcomes.

**Notebook 03 - Privacy & Governance** applies three privacy-preserving
techniques to the dataset: pseudonymization via salted SHA-256 hashing,
k-anonymity through quasi-identifier generalization and suppression, and
differential privacy via the Laplace mechanism. In addition to the
technical demonstrations, the notebook evaluates the dataset against key
GDPR requirements, identifying several privacy compliance gaps. The
analysis additionally classifies the NovaCred credit scoring model as
high-risk under EU AI Act Annex III, and highlights governance gaps,
including the absence of human oversight, no documented lawful basis per
data field, and no data retention schedule.

**Recommendations**

Nine governance recommendations are proposed across three domains.

-   On **data quality**: a deduplication pipeline to flag and remove
    duplicate records prior to ingestion, schema validation enforcing
    mandatory fields and data types at source, and standardized gender
    encoding restricted to two canonical values.

-   On **bias mitigation**: fairness reweighting via resampling to
    correct gender and age imbalances in training data, a multivariate
    fairness analysis controlling for financial variables (income, DTI,
    credit history) to distinguish true discriminatory patterns from
    legitimate risk differentials, and a recurring fairness audit
    process to monitor approval outcomes across protected groups
    following model updates.

-   On **GDPR and privacy**: documentation of the lawful basis per data
    field with applicant-facing transparency at the point of collection,
    pseudonymization of all direct identifiers prior to model training
    with secure retention of the mapping key, and a storage limitation
    policy mandating deletion or anonymization of applicant records once
    no longer needed, enforced through automated expiry triggers.

## Structure

-   'data/' - Dataset files
-   'notebooks/' - Jupyter analysis notebooks
-   'src/' - Python source code
-   'reports/' - Final deliverables / visualizations Additional files:
-   'PROJECT_MGMT.md' - Project management documentation
-   'REAMDE.md' - Project description & documentation

## [Project Management](PROJECT_MGMT.md)

Click on the link to open the Project Management file. It includes our
timeline, tasks, and latest meeting notes.

*All team members contributed to this repository. Commit history
available for verification.*
