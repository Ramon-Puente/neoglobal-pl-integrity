Primary Key: Stripe charge.id (ch_xxx).

ERP Match: NetSuite CustomerPayment.externalId must equal Stripe charge.id.

Memo Logic: NetSuite memo field must be prefixed with Stripe: {charge_id}.

Currency Handling: Use ISO 4217 (e.g., 'USD') and store amounts in DECIMAL(19, 4) to handle multi-currency aggregates without precision loss.

Account Codes: Revenue matches to Account 4000 (Subscription Revenue).