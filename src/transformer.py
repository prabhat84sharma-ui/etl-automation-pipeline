"""
Enterprise Data Transformation Engine.
Implements data normalization, business logic validation, currency conversion,
strict schema type enforcement, and Star Schema dimension/fact modeling.

Author: Prabhat Sharma (prabhat84sharma-ui)
"""

import logging
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
import numpy as np
import pandas as pd

from src.config import get_config

logger = logging.getLogger(__name__)


class TransformationError(Exception):
    """Raised when data transformation or business rule validation fails."""
    pass


class DataTransformer:
    """
    Transforms raw heterogeneous data into a clean, normalized,
    production-ready Star Schema ready for analytical queries and Power BI.
    """

    def __init__(self, target_currency: str = "USD"):
        self.config = get_config()
        self.target_currency = target_currency
        self.rates = self.config.get("transformer.rates", {
            "USD": 1.0000,
            "EUR": 1.0850,
            "GBP": 1.2720,
            "CAD": 0.7380,
            "AUD": 0.6540,
            "INR": 0.0120,
            "JPY": 0.0067,
        })
        self.business_rules = self.config.get("transformer.business_rules", {})
        self.max_discount_pct = float(self.business_rules.get("max_discount_pct", 70.0))
        self.default_tax_rate = float(self.business_rules.get("default_tax_rate", 0.0825))
        self.min_order_quantity = int(self.business_rules.get("min_order_quantity", 1))

    # =========================================================================
    # String & Data Normalization
    # =========================================================================
    @staticmethod
    def normalize_text(series: pd.Series, case: str = "title") -> pd.Series:
        """Cleans, trims whitespace, and applies uniform casing."""
        cleaned = series.astype(str).str.strip()
        cleaned = cleaned.replace(r"\s+", " ", regex=True)
        if case == "lower":
            return cleaned.str.lower()
        elif case == "upper":
            return cleaned.str.upper()
        elif case == "title":
            return cleaned.str.title()
        return cleaned

    @staticmethod
    def normalize_email(series: pd.Series) -> pd.Series:
        """Standardizes email formatting."""
        return series.astype(str).str.strip().str.lower()

    # =========================================================================
    # Currency Conversion
    # =========================================================================
    def convert_to_usd(self, amount_series: pd.Series, currency_series: pd.Series) -> Tuple[pd.Series, pd.Series]:
        """
        Converts multi-currency values to USD base currency using configured exchange rates.
        
        Returns:
            Tuple of (converted_amounts_usd, exchange_rates_applied)
        """
        rates_series = currency_series.astype(str).str.upper().map(self.rates)
        
        # Handle unrecognized currencies by falling back to 1.0 and logging warning
        unmapped_mask = rates_series.isna()
        if unmapped_mask.any():
            unknown_currencies = currency_series[unmapped_mask].unique()
            logger.warning("Unmapped currencies detected: %s. Using default 1.00 rate.", unknown_currencies)
            rates_series = rates_series.fillna(1.00)

        converted_usd = (amount_series.astype(float) * rates_series).round(4)
        return converted_usd, rates_series.round(6)

    # =========================================================================
    # Business Rules Application
    # =========================================================================
    def apply_order_business_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Applies data enrichment, discount clamping, and tax/total calculations.
        """
        logger.info("Applying business rules to %d order rows.", len(df))
        df_out = df.copy()

        # Enforce minimum quantity
        df_out["quantity"] = df_out["quantity"].fillna(self.min_order_quantity).astype(int)
        df_out["quantity"] = df_out["quantity"].apply(lambda q: max(self.min_order_quantity, q))

        # Clamp discount percentage
        if "discount_pct" not in df_out.columns:
            df_out["discount_pct"] = 0.0
        df_out["discount_pct"] = pd.to_numeric(df_out["discount_pct"], errors="coerce").fillna(0.0)
        df_out["discount_pct"] = df_out["discount_pct"].clip(lower=0.0, upper=self.max_discount_pct)

        # Standardize currency & convert prices to USD
        if "currency_code" not in df_out.columns:
            df_out["currency_code"] = "USD"
        df_out["currency_code"] = df_out["currency_code"].astype(str).str.upper()

        unit_price_usd, exchange_rates = self.convert_to_usd(
            df_out["unit_price_raw"], df_out["currency_code"]
        )
        df_out["exchange_rate_to_usd"] = exchange_rates
        df_out["unit_price_usd"] = unit_price_usd

        # Calculate financial metrics
        gross_amount = (df_out["quantity"] * df_out["unit_price_usd"]).round(4)
        df_out["gross_amount_usd"] = gross_amount

        discount_amount = (gross_amount * (df_out["discount_pct"] / 100.0)).round(4)
        df_out["discount_amount_usd"] = discount_amount

        taxable_amount = gross_amount - discount_amount
        tax_amount = (taxable_amount * self.default_tax_rate).round(4)
        df_out["tax_amount_usd"] = tax_amount

        total_amount = (taxable_amount + tax_amount).round(4)
        df_out["total_amount_usd"] = total_amount

        # Standardize Order Status
        status_map = {
            "COMPLETED": "COMPLETED",
            "COMPLETE": "COMPLETED",
            "PAID": "COMPLETED",
            "SUCCESS": "COMPLETED",
            "PENDING": "PENDING",
            "PROCESSING": "PENDING",
            "CANCELLED": "CANCELLED",
            "CANCELED": "CANCELLED",
            "REFUNDED": "REFUNDED",
        }
        df_out["order_status"] = (
            df_out["order_status"]
            .astype(str)
            .str.upper()
            .map(lambda s: status_map.get(s, "COMPLETED"))
        )

        return df_out

    # =========================================================================
    # Dimension Generation: dim_date (Calendar Dimension)
    # =========================================================================
    def generate_dim_date(self, start_date: date, end_date: date) -> pd.DataFrame:
        """
        Generates a comprehensive analytical Date Dimension.
        
        Args:
            start_date: Range start.
            end_date: Range end.
        """
        logger.info("Generating dim_date dimension from %s to %s", start_date, end_date)
        date_list = []
        curr = start_date
        while curr <= end_date:
            date_key = int(curr.strftime("%Y%m%d"))
            day_of_week = curr.isoweekday()  # 1 = Monday, 7 = Sunday
            is_weekend = day_of_week in (6, 7)
            quarter = (curr.month - 1) // 3 + 1
            
            # Fiscal year starting July 1st
            fiscal_year = curr.year if curr.month < 7 else curr.year + 1
            fiscal_quarter = f"FY{fiscal_year}-Q{((curr.month + 5) % 12) // 3 + 1}"

            date_list.append({
                "date_key": date_key,
                "full_date": curr,
                "day_of_week": day_of_week,
                "day_name": curr.strftime("%A"),
                "day_of_month": curr.day,
                "day_of_year": int(curr.strftime("%j")),
                "week_of_year": int(curr.strftime("%W")) + 1,
                "month_num": curr.month,
                "month_name": curr.strftime("%B"),
                "quarter": quarter,
                "quarter_name": f"Q{quarter}",
                "year_num": curr.year,
                "is_weekend": is_weekend,
                "fiscal_year": fiscal_year,
                "fiscal_quarter": fiscal_quarter,
            })
            curr += timedelta(days=1)

        df_dim_date = pd.DataFrame(date_list)
        return df_dim_date

    # =========================================================================
    # Dimension Generation: dim_customer
    # =========================================================================
    def build_dim_customer(self, raw_customers: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Transforms and normalizes customer profile data."""
        if raw_customers is None or raw_customers.empty:
            logger.info("Generating baseline customer dimension records.")
            raw_customers = pd.DataFrame([
                {"customer_id": f"CUST-{i:04d}", "first_name": f"Customer{i}", "last_name": f"Surname{i}",
                 "email": f"customer.{i}@enterprise.com", "city": "New York", "state": "NY", "country": "USA",
                 "postal_code": "10001", "customer_segment": "Enterprise" if i % 5 == 0 else "Standard"}
                for i in range(100, 160)
            ])

        df = raw_customers.copy()
        df["customer_id"] = df["customer_id"].astype(str).str.strip()
        df["first_name"] = self.normalize_text(df["first_name"], case="title")
        df["last_name"] = self.normalize_text(df["last_name"], case="title")
        df["email"] = self.normalize_email(df["email"])
        df["country"] = self.normalize_text(df.get("country", pd.Series(["USA"] * len(df))), case="upper")
        df["customer_segment"] = self.normalize_text(df.get("customer_segment", pd.Series(["Standard"] * len(df))), case="title")
        df["is_active"] = df.get("is_active", pd.Series([True] * len(df))).astype(bool)

        # Deduplicate on natural primary key
        df = df.drop_duplicates(subset=["customer_id"], keep="last")
        return df

    # =========================================================================
    # Dimension Generation: dim_product
    # =========================================================================
    def build_dim_product(self, raw_products: Optional[pd.DataFrame] = None) -> pd.DataFrame:
        """Transforms and normalizes product catalog data."""
        if raw_products is None or raw_products.empty:
            logger.info("Generating baseline product dimension records.")
            categories = ["Electronics", "Office Supplies", "Apparel", "Hardware", "Furniture"]
            brands = ["NexusCore", "ApexGear", "Vanguard", "TerraTech", "Lumina"]
            raw_products = pd.DataFrame([
                {
                    "product_id": f"PROD-{i:04d}",
                    "product_name": f"High-Performance Item {i}",
                    "category": categories[i % len(categories)],
                    "subcategory": f"SubCat-{(i % 3) + 1}",
                    "brand": brands[i % len(brands)],
                    "unit_cost_usd": round(15.0 + (i * 2.5), 2),
                    "unit_price_usd": round(35.0 + (i * 4.8), 2),
                    "status": "ACTIVE",
                }
                for i in range(200, 250)
            ])

        df = raw_products.copy()
        df["product_id"] = df["product_id"].astype(str).str.strip()
        df["product_name"] = self.normalize_text(df["product_name"], case="title")
        df["category"] = self.normalize_text(df["category"], case="title")
        df["subcategory"] = self.normalize_text(df.get("subcategory", pd.Series(["General"] * len(df))), case="title")
        df["brand"] = self.normalize_text(df.get("brand", pd.Series(["Generic"] * len(df))), case="title")
        df["unit_cost_usd"] = pd.to_numeric(df["unit_cost_usd"], errors="coerce").fillna(0.0).round(4)
        df["unit_price_usd"] = pd.to_numeric(df["unit_price_usd"], errors="coerce").fillna(0.0).round(4)
        df["status"] = df.get("status", pd.Series(["ACTIVE"] * len(df))).astype(str).str.upper()

        df = df.drop_duplicates(subset=["product_id"], keep="last")
        return df

    # =========================================================================
    # Dimension Generation: dim_channel
    # =========================================================================
    def build_dim_channel(self) -> pd.DataFrame:
        """Standard sales channel dimension."""
        return pd.DataFrame([
            {"channel_key": 1, "channel_id": "CH-01", "channel_name": "Shopify Storefront", "channel_type": "E-COMMERCE", "platform": "Shopify", "is_active": True},
            {"channel_key": 2, "channel_id": "CH-02", "channel_name": "Amazon Marketplace", "channel_type": "MARKETPLACE", "platform": "Amazon", "is_active": True},
            {"channel_key": 3, "channel_id": "CH-03", "channel_name": "Mobile Commerce App", "channel_type": "MOBILE_APP", "platform": "iOS/Android", "is_active": True},
            {"channel_key": 4, "channel_id": "CH-04", "channel_name": "Corporate B2B Portal", "channel_type": "B2B", "platform": "Direct Web", "is_active": True},
        ])

    # =========================================================================
    # Fact Table Generation: fact_orders
    # =========================================================================
    def build_fact_orders(
        self,
        raw_orders: pd.DataFrame,
        dim_customer: pd.DataFrame,
        dim_product: pd.DataFrame,
        dim_channel: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Maps natural keys to dimension foreign keys and creates fact_orders.
        """
        logger.info("Transforming raw orders into Star Schema fact_orders...")
        df = self.apply_order_business_rules(raw_orders)

        # Parse and format timestamps
        df["order_timestamp"] = pd.to_datetime(df["order_timestamp"], utc=True)
        df["date_key"] = df["order_timestamp"].dt.strftime("%Y%m%d").astype(int)

        # Map channel foreign key
        channel_id_to_key = dict(zip(dim_channel["channel_id"], dim_channel["channel_key"]))
        df["channel_key"] = df["channel_id"].map(channel_id_to_key).fillna(1).astype(int)

        # Map surrogate customer_key (generate 1-based index if customer_key not in dim)
        if "customer_key" not in dim_customer.columns:
            dim_customer["customer_key"] = range(1, len(dim_customer) + 1)
        cust_map = dict(zip(dim_customer["customer_id"], dim_customer["customer_key"]))
        df["customer_key"] = df["customer_id"].map(cust_map).fillna(1).astype(int)

        # Map surrogate product_key
        if "product_key" not in dim_product.columns:
            dim_product["product_key"] = range(1, len(dim_product) + 1)
        prod_map = dict(zip(dim_product["product_id"], dim_product["product_key"]))
        df["product_key"] = df["product_id"].map(prod_map).fillna(1).astype(int)

        # Deduplicate on composite natural key
        if "order_line_item_id" not in df.columns:
            df["order_line_item_id"] = df["order_id"] + "-L1"

        df = df.drop_duplicates(subset=["order_id", "order_line_item_id"], keep="last")

        fact_columns = [
            "order_id",
            "order_line_item_id",
            "customer_key",
            "product_key",
            "channel_key",
            "date_key",
            "order_timestamp",
            "quantity",
            "unit_price_raw",
            "currency_code",
            "exchange_rate_to_usd",
            "unit_price_usd",
            "discount_pct",
            "discount_amount_usd",
            "tax_amount_usd",
            "total_amount_usd",
            "order_status",
        ]
        return df[fact_columns]

    # =========================================================================
    # Aggregated Fact Generation: fact_daily_sales_summary
    # =========================================================================
    def build_daily_sales_summary(
        self, fact_orders: pd.DataFrame, dim_product: pd.DataFrame
    ) -> pd.DataFrame:
        """Rolls up order grains into daily aggregated KPI fact table."""
        prod_cat_map = dict(zip(dim_product["product_key"], dim_product["category"]))
        df = fact_orders.copy()
        df["category"] = df["product_key"].map(prod_cat_map).fillna("General")
        completed = df[df["order_status"] == "COMPLETED"]

        if completed.empty:
            return pd.DataFrame(columns=[
                "date_key", "channel_key", "category", "total_orders",
                "total_units_sold", "gross_revenue_usd", "total_discount_usd",
                "total_tax_usd", "net_revenue_usd", "average_order_value_usd"
            ])

        grouped = completed.groupby(["date_key", "channel_key", "category"]).agg(
            total_orders=("order_id", "nunique"),
            total_units_sold=("quantity", "sum"),
            gross_revenue_usd=("total_amount_usd", "sum"),
            total_discount_usd=("discount_amount_usd", "sum"),
            total_tax_usd=("tax_amount_usd", "sum"),
            net_revenue_usd=("total_amount_usd", "sum"),
            average_order_value_usd=("total_amount_usd", "mean"),
        ).reset_index()

        grouped["gross_revenue_usd"] = grouped["gross_revenue_usd"].round(4)
        grouped["total_discount_usd"] = grouped["total_discount_usd"].round(4)
        grouped["total_tax_usd"] = grouped["total_tax_usd"].round(4)
        grouped["net_revenue_usd"] = grouped["net_revenue_usd"].round(4)
        grouped["average_order_value_usd"] = grouped["average_order_value_usd"].round(4)

        return grouped

    # =========================================================================
    # Master Transformation Orchestrator
    # =========================================================================
    def transform_all(self, extracted_data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Transforms all raw extracted data into unified Star Schema tables.
        
        Args:
            extracted_data: Dictionary of raw DataFrames.
            
        Returns:
            Dictionary of modeled Star Schema DataFrames.
        """
        logger.info("Executing comprehensive Star Schema transformation pipeline...")

        # 1. Date Dimension (Covering past 1 year to future 1 year)
        today = date.today()
        dim_date = self.generate_dim_date(today - timedelta(days=365), today + timedelta(days=90))

        # 2. Dimensions: Customer, Product, Channel
        raw_cust = extracted_data.get("customers", extracted_data.get("dim_customer"))
        dim_customer = self.build_dim_customer(raw_cust)

        raw_prod = extracted_data.get("products", extracted_data.get("dim_product"))
        dim_product = self.build_dim_product(raw_prod)

        dim_channel = self.build_dim_channel()

        # 3. Fact Orders
        raw_orders = extracted_data.get("api_orders", extracted_data.get("orders", pd.DataFrame()))
        if raw_orders.empty:
            logger.warning("No raw orders found in extracted data. Generating sample orders for pipeline continuity.")
            raw_orders = pd.DataFrame([
                {
                    "order_id": f"ORD-DEMO-{i:05d}",
                    "order_line_item_id": f"ORD-DEMO-{i:05d}-L1",
                    "customer_id": f"CUST-0{100 + (i % 50):03d}",
                    "product_id": f"PROD-0{200 + (i % 40):03d}",
                    "channel_id": f"CH-0{(i % 4) + 1}",
                    "order_timestamp": (datetime.utcnow() - timedelta(hours=i * 2)).isoformat() + "Z",
                    "quantity": (i % 5) + 1,
                    "unit_price_raw": round(49.99 + (i * 3.75), 2),
                    "currency_code": ["USD", "EUR", "GBP", "CAD"][i % 4],
                    "discount_pct": [0.0, 10.0, 15.0, 20.0][i % 4],
                    "order_status": "COMPLETED",
                }
                for i in range(1, 151)
            ])

        fact_orders = self.build_fact_orders(raw_orders, dim_customer, dim_product, dim_channel)

        # 4. Aggregated Fact: Daily Summary
        fact_daily = self.build_daily_sales_summary(fact_orders, dim_product)

        logger.info(
            "Transformation complete. Generated: dim_date (%d), dim_customer (%d), dim_product (%d), "
            "dim_channel (%d), fact_orders (%d), fact_daily_sales_summary (%d).",
            len(dim_date), len(dim_customer), len(dim_product),
            len(dim_channel), len(fact_orders), len(fact_daily)
        )

        return {
            "dim_date": dim_date,
            "dim_customer": dim_customer,
            "dim_product": dim_product,
            "dim_channel": dim_channel,
            "fact_orders": fact_orders,
            "fact_daily_sales_summary": fact_daily,
        }
