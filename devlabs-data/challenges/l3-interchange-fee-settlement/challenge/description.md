# Interchange Fee Settlement Summary

Helix Payments is a payment network. One of their products is an interchange fee summary of all the merchants in a country and mode of card transaction.

Interchange is the fee on an approved card payment. The rate depends on merchant category (`mcc`), country (`country_code`), and how the card was presented (`entry_mode` — chip, swipe, or contactless). Each rate is valid only between `effective_from` and `effective_to`.

Only approved payments (`response_code == "00"`) are in scope. Each of those payments must be priced with the rate that matches `(mcc, country_code, entry_mode)` and whose window contains the payment date:

```
interchange_fee = round(amount * rate_bps / 10000 + fixed_fee, 2)
```

Approved payments with no matching rate are out of scope. Helix cannot publish a fee without a rate.

**Fact table.** The txn files contain transactions for the **first six months of 2024** only.

**Rate card.** It is unclear how completely or how far back the interchange rate schedule is maintained — use the version whose `effective_from` / `effective_to` window contains each payment date.

The output is one row per `(country_code, entry_mode)`:

| Column | Meaning |
| --- | --- |
| `txn_count` | How many payments were priced in the group |
| `total_fee` | Sum of `interchange_fee` in the group |
| `distinct_rate_versions_used` | How many distinct `rate_id`s were used in the group |
