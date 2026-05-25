# BrewTrack Analytics - Data Dictionary

**Gold Layer · Microsoft Fabric Lakehouse**
All tables described here are produced by the Silver-to-Gold PySpark notebook and served via two Power BI semantic models: the Sales Semantic Model and the Inventory Semantic Model.

---

## Table of contents

- [Data model overview](#data-model-overview)
- [Fact tables](#fact-tables)
  - [fact_sales](#fact_sales)
  - [fact_inventory](#fact_inventory)
- [Dimension tables](#dimension-tables)
  - [dim_customer](#dim_customer)
  - [dim_employee](#dim_employee)
  - [dim_product](#dim_product)
  - [dim_store](#dim_store)
- [Calendar tables](#calendar-tables)
  - [transaction_calendar](#transaction_calendar)
  - [baked_calendar](#baked_calendar)
- [Encoding reference](#encoding-reference)
- [Data quality notes](#data-quality-notes)

---

## Data model overview

```
SALES SEMANTIC MODEL
transaction_calendar (1) ──── (*) fact_sales (*) ──── (1) dim_product
                                       |
                               (*) ───────── (1) dim_customer
                               (*) ───────── (1) dim_employee
                               (*) ───────── (1) dim_store

INVENTORY SEMANTIC MODEL
baked_calendar (1) ──── (*) fact_inventory (*) ──── (1) dim_product
                                   |
                           (*) ───────── (1) dim_store
```

Relationships are all Many-to-One from the fact table to the dimension table.
Join keys are listed in each table below under the **Key** column.

---

## Fact tables

---

### fact_sales

**Description:** One row per line item sold across all three New York store locations. A single customer transaction may contain multiple line items (e.g. a latte and a pastry), each recorded as a separate row linked by `transaction_id`.

**Semantic model:** Sales Semantic Model
**Source:** `sales_by_store` source file, processed through Bronze and Silver lakehouses
**Grain:** One row per transaction line item
**Row count:** 1,000 (May 2018 sample)
**Null columns:** None

| Column | Data type | Key | Description | Example values | Range / Accepted values |
|---|---|---|---|---|---|
| `transaction_id` | INT | FK | Unique identifier for a customer transaction. Multiple rows may share the same `transaction_id` when a basket contains more than one product. | 1644, 1034, 2257 | 2 to 2853 |
| `transaction_date` | DATE | | The calendar date the transaction occurred. | 2018-05-01 | Format: yyyy-MM-dd |
| `transaction_time` | STRING | | The time the transaction was recorded at the till, to the second. | 7:03:30, 14:22:15 | Format: H:mm:ss |
| `store_id` | INT | FK → dim_store | Identifier for the store where the transaction took place. Joins to `dim_store[store_id]`. | 3, 5, 8 | 3, 5, 8 |
| `staff_id` | INT | FK → dim_employee | Identifier for the member of staff who processed the transaction. Joins to `dim_employee[staff_id]`. | 12, 30, 45 | 12 to 45 |
| `customer_id` | INT | FK → dim_customer | Identifier for the customer. Joins to `dim_customer[customer_id]`. | 5247, 5333, 5627 | 2 to 8501 |
| `instore_yn` | STRING | | Indicates whether the customer consumed the order in-store or took it away. | Y, N | Y = In-store · N = Takeaway |
| `order` | INT | | Order number within the transaction. Currently always 1 (single-order transactions only). | 1 | Always 1 in current dataset |
| `line_item_id` | INT | | Line item position within the order. Differentiates multiple products within the same transaction. | 1, 2, 3 | 1 to 5 |
| `product_id` | INT | FK → dim_product | Identifier for the product sold. Joins to `dim_product[product_id]`. | 22, 37, 69 | 22 to 87 |
| `quantity_sold` | INT | | Number of units of the product sold in this line item. | 1, 2 | 1 to 2 |
| `unit_price` | DECIMAL | | Retail price per unit at point of sale, in USD. | 2.00, 3.00, 4.75 | $2.00 to $4.75 |
| `promo_item_yn` | STRING | | Indicates whether the product was sold as part of a promotional offer. | N | Y = Promo · N = Full price. All N in current dataset. |
| `processing_date` | DATE | | Pipeline processing date. Used for incremental loading. Partitions data in the Silver lakehouse. | 2026-05-17 | Format: yyyy-MM-dd |
| `sales_key` | STRING | PK | SHA-256 surrogate primary key generated from `transaction_id`, `transaction_date`, `transaction_time`, `order`, and `line_item_id`. Guarantees row-level uniqueness for Delta MERGE operations. | 40096e32f069... | 64-character SHA-256 hex string |
| `transaction_date_id` | INT | FK → transaction_calendar | Integer representation of `transaction_date` used to join to `transaction_calendar[date_id]`. Format YYYYMMDD. | 20180501 | Format: YYYYMMDD |
| `time_of_day_category` | STRING | | Business-defined time band derived from `transaction_time`. Used for peak trading analysis and staffing decisions. | Morning, Afternoon, Lunch, Evening | Morning (06:00–11:59) · Lunch (12:00–13:59) · Afternoon (14:00–17:59) · Evening (18:00–23:59) |
| `revenue` | DECIMAL | | Total line item revenue in USD, calculated as `quantity_sold × unit_price`. | 2.00, 3.00, 9.50 | $2.00 to $9.50 |

---

### fact_inventory

**Description:** One row per product per store per baked date, tracking daily inventory from the start of day through to close. Covers baked goods only (product IDs 69 to 79). Used to measure waste, freshness, sell-through rate, and stockout risk.

**Semantic model:** Inventory Semantic Model
**Source:** `food_inventory` source file, processed through Bronze and Silver lakehouses
**Grain:** One row per product per store per baked date
**Row count:** 1,000 (2017–2019 sample)
**Null columns:** None

| Column | Data type | Key | Description | Example values | Range / Accepted values |
|---|---|---|---|---|---|
| `inventory_id` | STRING | PK | SHA-256 surrogate primary key generated from store, product, and baked date. Guarantees row-level uniqueness for Delta MERGE operations. | 2f01c92b6718... | 64-character SHA-256 hex string |
| `store_id` | INT | FK → dim_store | Identifier for the store holding the inventory. Joins to `dim_store[store_id]`. | 3, 5, 8 | 3, 5, 8 |
| `baked_date` | DATE | | The date the batch of goods was baked. Used to calculate `days_since_baked` and `is_fresh`. | 2016-12-24, 2017-01-03 | Format: yyyy-MM-dd |
| `transaction_date` | DATE | FK → baked_calendar | The calendar date the inventory record was observed. Joins to `baked_calendar[date_id]` via `transaction_date_id`. | 2017-01-02, 2017-01-04 | Format: yyyy-MM-dd |
| `product_id` | INT | FK → dim_product | Identifier for the baked product. Joins to `dim_product[product_id]`. Covers bakery product IDs only (69–79). | 69, 71, 74 | 69 to 79 |
| `quantity_start_of_day` | INT | | Number of units of this product available at the start of the trading day. | 18, 48 | 18 to 48 |
| `quantity_sold` | INT | | Number of units sold during the trading day. | 1, 5, 12 | 1 to 18 |
| `quantity_remaining` | INT | | Number of units left unsold at end of day. Calculated as `quantity_start_of_day − quantity_sold`. | 0, 16, 47 | 0 to 47 |
| `days_since_baked` | INT | | Number of days between `baked_date` and `transaction_date`. Used to determine freshness status. | 1, 7, 14 | 1 to 14 |
| `waste_quantity` | INT | | Number of units wasted (unsold and past freshness window). Equals `quantity_remaining` when `is_fresh = N`, else 0. | 0, 16, 47 | 0 to 47 |
| `sell_through_rate` | DECIMAL | | Proportion of stock sold on the day. Calculated as `quantity_sold ÷ quantity_start_of_day`. | 0.111, 0.556, 1.000 | 0.021 to 1.000 |
| `is_fresh` | STRING | | Indicates whether the product was still within its freshness window at the time of the record. Y if `days_since_baked = 1`, N otherwise. | Y, N | Y = Fresh (baked same day) · N = Stale |
| `stockout_flag` | STRING | | Indicates whether the store ran out of this product during the trading day. Y if `quantity_remaining = 0` and `quantity_sold > 0`. | Y, N | Y = Stockout occurred · N = No stockout |
| `baked_date_id` | INT | FK → baked_calendar | Integer representation of `baked_date` used to join to `baked_calendar[date_id]`. Format YYYYMMDD. | 20161224, 20170103 | Format: YYYYMMDD |
| `transaction_date_id` | INT | FK → baked_calendar | Integer representation of `transaction_date`. Primary join key to `baked_calendar[date_id]`. Format YYYYMMDD. | 20170102, 20170104 | Format: YYYYMMDD |
| `processing_date` | DATE | | Pipeline processing date. Used for incremental loading. | 2026-05-28 | Format: yyyy-MM-dd |

---

## Dimension tables

---

### dim_customer

**Description:** One row per registered customer. Contains demographic and loyalty information. Customers are linked to a home store and hold a loyalty card number. The `age` and `age_category` fields are derived in the Gold layer from `birthdate`.

**Semantic model:** Sales Semantic Model
**Source:** `customer_lookup` source file
**Grain:** One row per customer
**Row count:** 1,000
**Null columns:** `customer_first_name` (100% null in current dataset — excluded from reports)

| Column | Data type | Key | Description | Example values | Range / Accepted values |
|---|---|---|---|---|---|
| `customer_id` | INT | PK | Unique identifier for each registered customer. | 1, 2, 3 | 1 to 5198 |
| `home_store` | INT | FK → dim_store | The store ID of the customer's primary store. | 3, 5 | 3, 5 |
| `customer_first_name` | STRING | | Customer's first name. Fully null in current dataset. Retained for future data loads. | null | null |
| `customer_email` | STRING | | Customer's email address. Invalid format emails were blanked out during Silver quality checks. | venus@adipiscing.edu | Standard email format |
| `customer_since` | DATE | | Date the customer first registered with the loyalty programme. | 2017-01-04 | Format: yyyy-MM-dd |
| `loyalty_card_number` | STRING | | Unique loyalty card reference number assigned to the customer. | 908-424-2890 | Format: NNN-NNN-NNNN |
| `birthdate` | DATE | | Customer's date of birth. Used to derive `age` and `age_category`. | 1950-05-29 | Format: yyyy-MM-dd · 1950 to 2001 |
| `gender` | STRING | | Customer's gender. Standardised during Silver quality checks from multiple source encodings. | M, F, Other | M = Male · F = Female · Other |
| `birth_year` | INT | | Year component extracted from `birthdate`. Used for age band calculations. | 1950, 1985, 2001 | 1950 to 2001 |
| `age` | INT | | Customer's age in years, calculated in the Gold layer as current year minus `birth_year`. | 18, 35, 68 | 18 to 68 |
| `age_category` | STRING | | Age band derived from `age` in the Gold layer. Used for customer segment analysis in Power BI. | 36-50, 18-25 | 18-25 · 26-35 · 36-50 · 51-65 · 65+ |
| `processing_date` | DATE | | Pipeline processing date. Used for incremental loading. | 2026-05-28 | Format: yyyy-MM-dd |

---

### dim_employee

**Description:** One row per member of staff across all store locations. Contains position, tenure, and manager status. The `duration` and `duration_category` fields are derived in the Gold layer from `start_date`. Used in the Sales Semantic Model to analyse staff performance.

**Semantic model:** Sales Semantic Model
**Source:** `employee_lookup` source file
**Grain:** One row per employee
**Row count:** 25
**Null columns:** None

| Column | Data type | Key | Description | Example values | Range / Accepted values |
|---|---|---|---|---|---|
| `staff_id` | INT | PK | Unique identifier for each member of staff. | 6, 12, 30 | 6 to 45 |
| `first_name` | STRING | | Employee's first name. | Karen, Kelsey, Brittani | Free text |
| `last_name` | STRING | | Employee's last name. | Cupps, Cameron, Jorden | Free text |
| `full_name` | STRING | | Concatenation of `first_name` and `last_name`, generated in the Gold layer. Used as the display label in Power BI staff visuals. | Karen Cupps, Brittani Jorden | Free text |
| `position` | STRING | | Job title of the employee. | Store Manager, Coffee Wrangler | Store Manager · Coffee Wrangler |
| `is_manager` | STRING | | Derived flag indicating whether the employee holds a Store Manager position. Y if `position = Store Manager`, else N. | Y, N | Y = Manager · N = Not a manager |
| `start_date` | DATE | | Date the employee joined the business. Used to calculate `duration`. | 2016-07-24, 2003-10-18 | Format: yyyy-MM-dd |
| `duration` | INT | | Number of full years the employee has been with the business, calculated in the Gold layer from `start_date` to today's date. | 0, 7, 17 | 0 to 17 |
| `duration_category` | STRING | | Tenure band derived from `duration` in the Gold layer. Used to analyse whether longer-serving staff generate more revenue. | 1-3yrs, 10+ yrs | 0-1yrs · 1-3yrs · 3-5yrs · 7-10yrs · 10+ yrs |
| `store_id` | INT | FK → dim_store | The store ID the employee is assigned to. Joins to `dim_store[store_id]`. | 3, 5, 8 | 3 to 10 |
| `processing_date` | DATE | | Pipeline processing date. Used for incremental loading. | 2026-05-28 | Format: yyyy-MM-dd |

---

### dim_product

**Description:** One row per product in the full catalogue. Covers all product groups including beverages, baked food, whole bean and loose tea, merchandise, and add-ons. Used in both semantic models. Pricing, margin, and classification fields are as at the most recent processing date.

**Semantic model:** Sales Semantic Model · Inventory Semantic Model
**Source:** `product_lookup` source file
**Grain:** One row per product
**Row count:** 80
**Null columns:** `product_lb` (18 nulls — applicable to weighted products only)

| Column | Data type | Key | Description | Example values | Range / Accepted values |
|---|---|---|---|---|---|
| `product_id` | INT | PK | Unique identifier for each product. | 1, 22, 69, 87 | 1 to 87 |
| `product_group` | STRING | | Top-level product classification. The broadest grouping, used for high-level revenue analysis. | Beverages, Food, Whole Bean/Teas | Beverages · Food · Merchandise · Whole Bean/Teas · Add-ons |
| `product_category` | STRING | | Mid-level classification within a product group. | Coffee, Tea, Bakery, Flavours | Coffee · Tea · Drinking Chocolate · Bakery · Branded · Coffee beans · Loose Tea · Packaged Chocolate · Flavours |
| `product_type` | STRING | | Sub-category within a product category. | Organic Beans, Herbal tea, Scone | 29 distinct types |
| `product` | STRING | | Full product name as used in reporting and on receipts. | Ethiopia Lg, Latte Rg, Cappuccino | 80 distinct product names |
| `product_description` | STRING | | Marketing description for the product. Not used in analytics but retained for completeness. | "It's like Carnival in a cup." | Free text · 51 distinct descriptions |
| `current_cost` | DECIMAL | | Unit cost to the business in USD (cost of goods sold basis). | 0.20, 3.60, 9.00 | $0.20 to $9.00 |
| `current_wholesale_price` | DECIMAL | | Wholesale price per unit in USD. | 0.30, 8.00, 36.00 | $0.30 to $36.00 |
| `current_retail_price` | DECIMAL | | Retail price per unit charged to customers in USD. | 0.76, 2.00, 45.00 | $0.76 to $45.00 |
| `gross_margin_pct` | DECIMAL | | Gross margin expressed as a decimal. Calculated as `(current_retail_price − current_cost) ÷ current_retail_price`. | 0.20, 0.40, 0.68 | 0.20 to 0.68 (20% to 68%) |
| `tax_exempt_yn` | STRING | | Indicates whether the product is exempt from sales tax. | Y, N | Y = Tax exempt · N = Taxable |
| `promo_yn` | STRING | | Indicates whether the product is currently on promotion. | N | Y = On promotion · N = Full price. All N in current dataset. |
| `new_product_yn` | STRING | | Indicates whether the product was newly introduced in the current period. | N | Y = New · N = Existing. All N in current dataset. |
| `product_original_uom` | STRING | | Unit of measure used for the product in source data. | oz, lb, each | oz · lb · each · serving |
| `product_lb` | DECIMAL | | Product weight in pounds. Applicable to whole bean and packaged products sold by weight. Null for non-weighted products. | 0.0563, 0.75, 1.50 | 0.0563 to 1.50 · null for non-weighted |
| `processing_date` | DATE | | Pipeline processing date. Used for incremental loading. | 2026-05-28 | Format: yyyy-MM-dd |

---

### dim_store

**Description:** One row per active retail store location. All three stores are located in New York State. Contains physical address, geolocation coordinates, store size, and the assigned store manager reference. The `neighorhood` column retains the original source spelling.

**Semantic model:** Sales Semantic Model · Inventory Semantic Model
**Source:** `store_lookup` source file
**Grain:** One row per store
**Row count:** 3
**Null columns:** None

| Column | Data type | Key | Description | Example values | Range / Accepted values |
|---|---|---|---|---|---|
| `store_id` | INT | PK | Unique identifier for each store location. | 3, 5, 8 | 3, 5, 8 |
| `store_type` | STRING | | Classification of the store operating model. | retail | retail (only type in current dataset) |
| `store_square_feet` | INT | | Total floor area of the store in square feet. | 900, 1300, 1500 | 900 to 1500 |
| `store_address` | STRING | | Street address of the store. | 100 Church Street | Free text |
| `store_city` | STRING | | City the store is located in. | New York, Long Island City | New York · Long Island City |
| `store_state_province` | STRING | | US state abbreviation. | NY | NY |
| `store_postal_code` | INT | | US zip code of the store. | 10007, 10036, 11106 | 10007 · 10036 · 11106 |
| `store_longitude` | DECIMAL | | WGS84 longitude coordinate of the store. Used for map visuals in Power BI. | -73.924008, -74.010130 | -74.010 to -73.924 |
| `store_latitude` | DECIMAL | | WGS84 latitude coordinate of the store. Used for map visuals in Power BI. | 40.713290, 40.761887 | 40.713 to 40.762 |
| `manager` | DECIMAL | | `staff_id` of the Store Manager assigned to this location. Links to `dim_employee[staff_id]`. Stored as DECIMAL due to source type — cast to INT in reports. | 6.0, 16.0, 31.0 | Corresponds to staff_id values |
| `neighorhood` | STRING | | Neighbourhood name for the store location. Note: column name retains original source spelling (missing 'i'). Used as the primary store display label in Power BI. | Astoria, Lower Manhattan, Hell's Kitchen | Astoria · Lower Manhattan · Hell's Kitchen |
| `store_full_address` | STRING | | Concatenated full address string combining street, city, state, and postal code. | 100 Church Street,New York,NY,10007 | Format: address,city,state,postcode |
| `processing_date` | DATE | | Pipeline processing date. Used for incremental loading. | 2026-05-28 | Format: yyyy-MM-dd |

---

## Calendar tables

Both calendar tables share an identical schema. They differ only in the date ranges they cover and the fact tables they are used with.

| Table | Used with | Date range covered |
|---|---|---|
| `transaction_calendar` | fact_sales | Feb 2017 to Nov 2019 (1,000 dates) |
| `baked_calendar` | fact_inventory | Feb 2017 to Nov 2019 (1,000 dates) |

---

### transaction_calendar

**Description:** Date dimension table used in the Sales Semantic Model. Each row represents one calendar date. Mark this table as a Date Table in Power BI (Date column = `Date`) before using time intelligence DAX functions.

**Semantic model:** Sales Semantic Model
**Grain:** One row per calendar date
**Row count:** 1,000

| Column | Data type | Key | Description | Example values | Range / Accepted values |
|---|---|---|---|---|---|
| `date_id` | INT | PK | Integer surrogate key for the date in YYYYMMDD format. Primary join key from `fact_sales[transaction_date_id]`. | 20170205, 20181231 | Format: YYYYMMDD · 20170205 to 20191101 |
| `Date` | DATE | | Full calendar date. Mark this column as the Date Table key in Power BI. | 2017-02-05 | Format: yyyy-MM-dd |
| `FiscalYear` | INT | | Fiscal year number. | 2017, 2018, 2019 | 2017 to 2019 |
| `FiscalQuarter` | INT | | Quarter number within the fiscal year. | 1, 2, 3, 4 | 1 to 4 |
| `FiscalMonthNumber` | INT | | Month number within the fiscal year (1 = January). | 1, 6, 12 | 1 to 12 |
| `FiscalMonthOfQuarter` | INT | | Month position within the current quarter (1 = first month of quarter). | 1, 2, 3 | 1 to 3 |
| `FiscalWeekOfYear` | INT | | ISO week number within the fiscal year. | 1, 26, 52 | 1 to 52 |
| `FiscalMonthName` | STRING | | Full month name. | January, February, December | January to December |
| `FiscalMonthYear` | STRING | | Abbreviated month and year label. Used as the x-axis label in time series charts. | Jan-17, Jun-18, Dec-19 | Format: Mon-YY |
| `FiscalQuarterYear` | INT | | Composite key combining quarter and year for quarter-over-quarter analysis. | 12017, 42018 | Format: QYYYY |
| `DayOfMonthNumber` | INT | | Day number within the calendar month. | 1, 15, 31 | 1 to 31 |
| `DayOfWeek` | INT | | Day of the week as an integer. 0 = Sunday, 6 = Saturday. | 0, 1, 6 | 0 (Sunday) to 6 (Saturday) |
| `DayName` | STRING | | Full day name. | Sunday, Monday, Saturday | Sunday to Saturday |

---

### baked_calendar

**Description:** Date dimension table used in the Inventory Semantic Model. Identical schema to `transaction_calendar`. Each row represents one calendar date. Mark this table as a Date Table in Power BI (Date column = `Date`) before using time intelligence DAX functions such as DATESMTD and DATEADD.

**Semantic model:** Inventory Semantic Model
**Grain:** One row per calendar date
**Row count:** 1,000

Columns are identical to `transaction_calendar`. See above for full column descriptions.

---

## Encoding reference

### Boolean-style flags (Y / N columns)

The following columns use Y/N string encoding rather than boolean. This is intentional to preserve compatibility with the source system encoding and to display cleanly in Power BI without requiring format overrides.

| Column | Table | Y means | N means |
|---|---|---|---|
| `instore_yn` | fact_sales | Customer consumed in-store | Customer took away |
| `promo_item_yn` | fact_sales | Item sold at promotional price | Item sold at full price |
| `is_fresh` | fact_inventory | Product baked within 1 day of transaction date | Product baked more than 1 day before transaction date |
| `stockout_flag` | fact_inventory | Store ran out of this product on this day | No stockout on this day |
| `tax_exempt_yn` | dim_product | Product is exempt from sales tax | Product is taxable |
| `promo_yn` | dim_product | Product is currently on promotion | Product at standard price |
| `new_product_yn` | dim_product | Product is newly introduced | Existing product |
| `is_manager` | dim_employee | Employee holds Store Manager position | Employee is not a manager |

### Gender encoding (dim_customer)

| Value | Meaning |
|---|---|
| M | Male |
| F | Female |
| Other | Non-binary or not specified |

Standardised during Silver quality checks from source values including `m`, `M`, `male`, `Male`, `MALE`, `f`, `F`, `female`.

### time_of_day_category encoding (fact_sales)

| Value | Transaction time window |
|---|---|
| Morning | 06:00 to 11:59 |
| Lunch | 12:00 to 13:59 |
| Afternoon | 14:00 to 17:59 |
| Evening | 18:00 to 23:59 |

### age_category encoding (dim_customer)

| Value | Age range |
|---|---|
| 18-25 | 18 to 25 years old |
| 26-35 | 26 to 35 years old |
| 36-50 | 36 to 50 years old |
| 51-65 | 51 to 65 years old |
| 65+ | 66 years and over |

### duration_category encoding (dim_employee)

| Value | Tenure range |
|---|---|
| 0-1yrs | Less than 1 year with the business |
| 1-3yrs | 1 to 3 years |
| 3-5yrs | 3 to 5 years |
| 7-10yrs | 7 to 10 years |
| 10+ yrs | More than 10 years |

---

## Data quality notes

The following known data quality issues exist in the Gold layer. All were identified and handled during Silver processing.

| Table | Column | Issue | How it was handled |
|---|---|---|---|
| dim_customer | customer_first_name | 100% null in current dataset | Column retained in schema for future data loads. Excluded from all Power BI visuals. |
| dim_store | neighorhood | Misspelled column name (missing 'i') | Retained as-is to match source system. Use `dim_store[neighorhood]` exactly in DAX and Power BI field wells. |
| dim_store | manager | Stored as DECIMAL rather than INT | Cast to INT in Power BI via column formatting. Source encoding preserved in Gold. |
| dim_product | product_lb | 18 null values | Null for non-weighted products (beverages, merchandise). Expected and valid. |
| fact_sales | promo_item_yn | All values are N in May 2018 dataset | No active promotions ran in this month. Column retained for completeness. |
| fact_sales | order | All values are 1 in current dataset | Single-order transactions only. Multi-order support exists in schema. |
| fact_inventory | stockout_flag | Only 2 Y values across 1,000 records | Low stockout rate (0.2%) is genuine data, not a processing error. The business over-supplies. |


