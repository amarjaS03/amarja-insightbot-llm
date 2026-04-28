# GPT Model Cost Comparison Report

**Project:** InsightBot Backend
**Date:** April 14, 2026
**Currency:** USD & INR (1 USD = ₹85)

---

## 1. Pricing Per 1K Tokens

### Old Models (4.x — Pre-Upgrade)

| Model | Use Case | Input (USD) | Input (INR) | Output (USD) | Output (INR) |
|-------|----------|-------------|-------------|--------------|--------------|
| gpt-4.1 | Deep | $0.0020 | ₹0.170 | $0.0080 | ₹0.680 |
| gpt-4.1-mini | Shallow | $0.0004 | ₹0.034 | $0.0016 | ₹0.136 |
| gpt-4o-mini | Legacy | $0.00015 | ₹0.013 | $0.0006 | ₹0.051 |

### New Models (5.x — Post-Upgrade)

| Model | Use Case | Input (USD) | Input (INR) | Output (USD) | Output (INR) |
|-------|----------|-------------|-------------|--------------|--------------|
| gpt-5.4 | Deep | $0.0025 | ₹0.213 | $0.0150 | ₹1.275 |
| gpt-5.4-mini | Shallow | $0.00075 | ₹0.064 | $0.0045 | ₹0.383 |
| gpt-5.4-nano | Lightweight | $0.0002 | ₹0.017 | $0.00125 | ₹0.106 |

---

## 2. Price Change: Old vs New (Equivalent Models)

| Mode | Old Model | New Model | Input Change | Output Change |
|------|-----------|-----------|--------------|---------------|
| Deep | gpt-4.1 | gpt-5.4 | +25% | +87.5% |
| Shallow | gpt-4.1-mini | gpt-5.4-mini | +87.5% | +181% |

---

## 3. Practical Cost Estimates

### Per Request — 10K Tokens (7K Input + 3K Output)

| Mode | Old Model | Old Cost (USD) | Old Cost (INR) | New Model | New Cost (USD) | New Cost (INR) | Increase |
|------|-----------|----------------|----------------|-----------|----------------|----------------|----------|
| Deep | gpt-4.1 | $0.038 | ₹3.23 | gpt-5.4 | $0.063 | ₹5.32 | +65% |
| Shallow | gpt-4.1-mini | $0.008 | ₹0.65 | gpt-5.4-mini | $0.019 | ₹1.60 | +146% |
| Nano (new) | — | — | — | gpt-5.4-nano | $0.005 | ₹0.44 | N/A |

### Per 100 Requests

| Mode | Old Cost (INR) | New Cost (INR) | Increase |
|------|----------------|----------------|----------|
| Deep | ₹323 | ₹532 | +₹209 |
| Shallow | ₹65 | ₹160 | +₹95 |

### Per 1,000 Requests

| Mode | Old Cost (INR) | New Cost (INR) | Increase |
|------|----------------|----------------|----------|
| Deep | ₹3,230 | ₹5,320 | +₹2,090 |
| Shallow | ₹650 | ₹1,600 | +₹950 |

---

## 4. Mode Mapping

| Mode | Endpoint | Model (Old) | Model (New) |
|------|----------|-------------|-------------|
| Insight Bot — Deep | POST /analyze_job | gpt-4.1 | gpt-5.4 |
| Insight Bot — Shallow | POST /analyze_job | gpt-4.1-mini | gpt-5.4-mini |
| Standard Bot (Simple QnA) | POST /simpleqna/analyze_job | gpt-4.1-mini | gpt-5.4-mini |

---

## 5. Cost Optimization Measures (Post-Upgrade)

To offset the increased per-token cost of 5.x models, the following pipeline optimizations were applied for **shallow mode**:

| Optimization | Deep | Shallow | Impact |
|--------------|------|---------|--------|
| EDA Iterations | 3 | 1 | ~66% fewer EDA LLM calls |
| EDA Tasks | Up to 5 | Max 3 | ~40% fewer code executions |
| Hypotheses Tested | All generated | Max 1 | ~50-66% fewer hypothesis LLM calls |
| Image Generation | Yes (charts, graphs) | No (tables/CSV only) | Eliminates vision API calls |
| Vision Analysis | Yes | Skipped | Saves ~600 tokens per image |
| Report Frame Tokens | 2,000 | 1,000 | 50% reduction |
| Final Report Tokens | 6,000 | 3,000 | 50% reduction |
| EDA Synthesis Tokens | 1,000 | 500 | 50% reduction |
| Query Analysis Tokens | 1,500 | 800 | ~47% reduction |

**Estimated net effect:** Shallow mode uses ~60-70% fewer total tokens than deep mode, keeping operational costs comparable to the old 4.1-mini pricing despite higher per-token rates.

---

## 6. Key Takeaways

1. **Output tokens are the biggest cost driver** — 5.x output pricing increased 87-181% over 4.x equivalents.
2. **Shallow mode optimizations are critical** — without them, shallow would cost 2-3x more than old pricing.
3. **gpt-5.4-nano** is available as an even cheaper option (₹0.44 per 10K tokens) for future lightweight use cases.
4. **Deep mode cost increase is moderate** (+65%) and justified by improved model capability.

---

*Generated from InsightBot Backend cost analysis — execution_api.py calculate_costs function*
