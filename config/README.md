# Configuration Files

This folder contains all configuration files for the ERP Intelligence Layer.

## Files

### 1. `universal_schema.yaml`
Defines the standard data format for all ERP systems.

**Purpose**: 
- Standardize data from different ERPs (GoFrugal, Tally, etc.)
- Define data types and validation rules
- Map ERP-specific fields to universal fields

**Key Sections**:
- `entities`: Core data tables (sales, inventory, customers, products)
- `erp_mappings`: How to convert ERP-specific field names
- `data_quality_rules`: Validation checks

### 2. `industry_benchmarks.yaml`
Industry-specific business rules and benchmarks.

**Purpose**:
- Define what "good" vs "bad" looks like for each industry
- Set thresholds for alerts (e.g., what is "dead stock"?)
- Capture domain expert knowledge

**Key Sections**:
- `industries`: Different business types (pumps, pharma, etc.)
- `alert_thresholds`: When to trigger alerts
- `messaging`: WhatsApp message formatting rules

### 3. `alert_rules.yaml`
Rules for when and how to generate alerts.

**Purpose**:
- Define conditions for each alert type
- Set severity levels
- Create message templates

**Key Sections**:
- `rules`: Individual alert definitions
- `delivery`: How/when to send alerts

## How to Use

When processing CSV data:
1. Load `universal_schema.yaml` to understand expected format
2. Use `erp_mappings` section to convert your CSV columns
3. Apply `data_quality_rules` to validate data
4. Use `industry_benchmarks.yaml` and `alert_rules.yaml` to generate alerts

## Example

If your CSV has a column called `BillNo` (from GoFrugal):
```yaml
# In universal_schema.yaml → erp_mappings → gofrugal
transaction_id: "BillNo"
