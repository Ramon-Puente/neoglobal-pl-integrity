# Step 1: The 60-Second Health Check

At the start of the day, a Financial Analyst opens the dashboard. Their first glance is at the three main KPIs at the top:

* **Total Exposure ($)**: Is this number large? This is the total dollar amount at risk due to discrepancies. It immediately tells them the financial materiality of the problem.
* **Data Integrity Score %**: Is this below 100%? This is the trigger. If it's not perfect, they know there's work to do.
* **Month-End Time Saved**: This is a constant reminder of the efficiency gained by not having to do this manually.

If exposure is $0 and integrity is 100%, their work is done. They can confidently move on to other tasks.

# Step 2: Triage the Problem

If there's an issue, the analyst uses the Reconciliation Overview pie chart to understand the nature of the problem at a high level.

* Is it mostly **Mismatches**? This suggests that data is flowing between systems, but there might be rounding, currency, or tax calculation issues.
* Is it mostly **Missing Records**? This is often more serious. It implies that the data sync between Stripe and the ERP (NetSuite) is failing for some transactions.

# Step 3: Investigate with the Forensic Table

This is where the action happens. The analyst scrolls down to the "Forensic Review: Anomalies for Auditor Review" table. This searchable dataframe is their main tool.

* **Action 1: Prioritize by Impact**. The first action is to sort the variance column from largest to smallest. This immediately brings the most expensive errors to the top. The team can now focus their energy on fixing the $10,000 error, not the $0.01 one.
* **Action 2: Isolate the Root Cause**. Based on the triage in Step 2, they can filter the table.
    * To see all missing records, they filter for `is_missing_in_erp = True`.
    * To see all amount mismatches, they filter for `is_amount_mismatch = True`.

# Step 4: Take Targeted Action

The dashboard's job is to turn a vague problem ("the numbers don't match") into a specific, actionable task. Here are the concrete actions the team can now take:

* **If a record is `is_missing_in_erp`**:
    * **The Old Way**: "I think some Stripe charges are missing from NetSuite this week." (Vague, requires a massive manual search).
    * **The New Way (Action)**: The analyst copies the `reconciliation_id` (e.g., the Stripe Charge ID) directly from the dashboard. They go straight to the engineering or finance systems team and say, "Stripe charge `ch_xyz` from yesterday is missing from NetSuite. Can you investigate why the sync failed for this specific transaction?" This reduces investigation time from hours to minutes.

* **If a record has an `is_amount_mismatch`**:
    * **The Old Way**: "The month-end revenue is off by $52.37, and I have no idea why."
    * **The New Way (Action)**: The analyst sorts by variance and sees a pattern of $0.01 discrepancies for a certain product type. They can now confidently go to the product team and say, "It looks like the new 'Pro Plan' has a rounding discrepancy between the Stripe checkout and how the invoice is recorded in NetSuite. Can we check the tax or rounding logic?"

---

In summary, the dashboard empowers the finance team to stop being spreadsheet detectives and start being strategic partners to the business. They can identify risks faster, provide engineers with exact details to fix bugs, and spend their time analyzing why errors occur, not just finding them.