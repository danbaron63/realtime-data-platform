# Data Generator

This package simulates events in a bank.

## Approach

We continuously generate random events from a predetermined list of events.
According to the chosen event, random data is generated for that event according to its schema.
According to the event and its schema, a number of entities may be affected.

## Events

| Event                      | Description                                               |
|----------------------------|-----------------------------------------------------------|
| `customer_created`         | A new customer registers with the bank.                   |
| `customer_details_updated` | Customer changes email, phone, address, employment, etc.  |
| `account_opened`           | A new bank account is opened.                             |
| `card_issued`              | A debit or credit card is issued for an account.          |
| `device_registered`        | Customer logs in from a new device for the first time.    |  
| `login_attempted`          | Mobile/web banking login attempt succeeds or fails.       | 
| `merchant_onboarded`       | A new merchant joins the acquiring network.               | 
| `payment_authorised`       | Card transaction is authorised at a merchant.             |
| `atm_withdrawal`           | Cash withdrawal occurs.                                   |
| `transfer_completed`       | Transfer successfully completes.                          |



