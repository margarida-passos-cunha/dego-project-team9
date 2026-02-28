"""
Functions for loading the raw JSON dataset and flattening the nested
structure into a pandas DataFrame that the rest of the team can work with.
"""

import json
import pandas as pd


def load_raw_json(filepath: str) -> list:
    """Load the raw JSON file and return the list of records."""
    with open(filepath, "r") as f:
        data = json.load(f)
    print(f"Loaded {len(data)} records from {filepath}")
    return data


def flatten_record(record: dict) -> dict:
    """
    Take a single nested JSON record and flatten it into a flat dictionary.
    
    The raw data has nested objects for applicant_info, financials, and decision,
    plus spending_behavior as an array. We flatten the nested objects into
    prefixed columns and aggregate spending_behavior into total + categories.
    """
    flat = {}

    # top-level fields
    flat["app_id"] = record.get("_id")

    # applicant_info fields
    app_info = record.get("applicant_info", {})
    flat["full_name"] = app_info.get("full_name")
    flat["email"] = app_info.get("email")
    flat["ssn"] = app_info.get("ssn")
    flat["ip_address"] = app_info.get("ip_address")
    flat["gender"] = app_info.get("gender")
    flat["date_of_birth"] = app_info.get("date_of_birth")
    flat["zip_code"] = app_info.get("zip_code")

    # financials
    fin = record.get("financials", {})
    flat["annual_income"] = fin.get("annual_income")
    flat["credit_history_months"] = fin.get("credit_history_months")
    flat["debt_to_income"] = fin.get("debt_to_income")
    flat["savings_balance"] = fin.get("savings_balance")

    # spending behavior — flatten the array
    spending = record.get("spending_behavior", [])
    flat["spending_total"] = sum(item.get("amount", 0) for item in spending)
    flat["spending_categories"] = len(spending)
    # store individual categories so we can analyze them later
    categories = [item.get("category", "") for item in spending]
    flat["spending_category_list"] = "|".join(categories)

    # decision
    dec = record.get("decision", {})
    flat["loan_approved"] = dec.get("loan_approved")
    flat["interest_rate"] = dec.get("interest_rate")
    flat["approved_amount"] = dec.get("approved_amount")
    flat["rejection_reason"] = dec.get("rejection_reason")

    # extra fields that appear inconsistently
    flat["processing_timestamp"] = record.get("processing_timestamp")
    flat["loan_purpose"] = record.get("loan_purpose")
    flat["notes"] = record.get("notes")

    return flat


def load_and_flatten(filepath: str) -> pd.DataFrame:
    """
    Main entry point: loads raw JSON and returns a flattened DataFrame.
    This is the function that the notebooks should call.
    """
    raw_data = load_raw_json(filepath)
    flat_records = [flatten_record(r) for r in raw_data]
    df = pd.DataFrame(flat_records)
    print(f"DataFrame shape: {df.shape}")
    return df


if __name__ == "__main__":
    # quick test
    df = load_and_flatten("data/raw_credit_applications.json")
    print(df.head())
    print(df.dtypes)
