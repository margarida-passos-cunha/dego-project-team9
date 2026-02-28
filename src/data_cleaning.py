"""
Data cleaning and remediation pipeline for the NovaCred credit applications dataset.

Provides functions for common data quality remediations such as
deduplication, value standardization, type coercion, invalid-value
handling, and validation checks. Each function is designed to be
run as part of the clean_pipeline() at the bottom of this module.
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate records based on app_id.
    
    Strategy: first drop rows whose 'notes' field indicates they are
    a known duplicate or resubmission, then fall back to keeping the
    first occurrence of any remaining ID duplicates.
    """
    before = len(df)
    
    # drop rows where notes indicate they are duplicates
    mask = df["notes"].isin(["DUPLICATE_ENTRY_ERROR", "RESUBMISSION"])
    df = df[~mask].copy()
    
    # safety net: if there are still any ID duplicates, keep the first
    df = df.drop_duplicates(subset=["app_id"], keep="first")
    
    after = len(df)
    print(f"Removed {before - after} duplicate records ({before} -> {after})")
    return df


def standardize_gender(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize gender values to a consistent format.
    
    Maps abbreviated and full-length labels to 'Male'/'Female',
    and treats empty strings or None as NaN.
    """
    gender_map = {
        "Male": "Male",
        "M": "Male",
        "Female": "Female",
        "F": "Female",
        "": np.nan,
        None: np.nan,
    }
    
    df["gender_original"] = df["gender"].copy()  # keep original for audit trail
    df["gender"] = df["gender"].map(gender_map)
    
    counts = df["gender"].value_counts(dropna=False)
    print(f"Gender standardization complete:\n{counts}\n")
    return df


def normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize date_of_birth to a consistent datetime format.
    
    Handles multiple date formats that may appear in the data
    (YYYY-MM-DD, DD/MM/YYYY, YYYY/MM/DD, MM/DD/YYYY). When the
    format is ambiguous (both parts <= 12), defaults to DD/MM/YYYY.
    """
    def parse_dob(val):
        if pd.isna(val) or val == "" or val == "None":
            return pd.NaT
        val = str(val).strip()
        if not val:
            return pd.NaT
        
        # ISO format: YYYY-MM-DD
        if "-" in val:
            try:
                return pd.to_datetime(val, format="%Y-%m-%d")
            except (ValueError, TypeError):
                return pd.NaT
        
        # slash-based formats
        if "/" in val:
            parts = val.split("/")
            if len(parts) != 3:
                return pd.NaT
            
            # YYYY/MM/DD — first part is 4 digits (year)
            if len(parts[0]) == 4:
                try:
                    return pd.to_datetime(val, format="%Y/%m/%d")
                except (ValueError, TypeError):
                    return pd.NaT
            
            # now it's either DD/MM/YYYY or MM/DD/YYYY
            first, second = int(parts[0]), int(parts[1])
            
            # if first > 12, it must be DD (can't be a month)
            if first > 12:
                try:
                    return pd.to_datetime(val, format="%d/%m/%Y")
                except (ValueError, TypeError):
                    return pd.NaT
            
            # if second > 12, it must be DD, so first is MM
            if second > 12:
                try:
                    return pd.to_datetime(val, format="%m/%d/%Y")
                except (ValueError, TypeError):
                    return pd.NaT
            
            # ambiguous case: both <= 12
            # default to DD/MM/YYYY (more common in the dataset)
            try:
                return pd.to_datetime(val, format="%d/%m/%Y")
            except (ValueError, TypeError):
                return pd.NaT
        
        return pd.NaT

    df["date_of_birth_original"] = df["date_of_birth"].copy()
    df["date_of_birth"] = df["date_of_birth"].apply(parse_dob)
    
    valid = df["date_of_birth"].notna().sum()
    missing = df["date_of_birth"].isna().sum()
    print(f"Date normalization: {valid} valid, {missing} missing/unparseable")
    return df


def fix_income_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Coerce annual_income to a numeric type.
    
    Some values may be stored as strings or have excessive decimal
    precision. Converts everything to numeric and rounds to whole numbers.
    """
    df["annual_income"] = pd.to_numeric(df["annual_income"], errors="coerce")
    
    # round to whole number since income should be integer
    df["annual_income"] = df["annual_income"].round(0)
    
    missing = df["annual_income"].isna().sum()
    print(f"Income type fix: {missing} missing values remain")
    return df


def fix_invalid_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Set impossible values to NaN.
    
    Credit history months and savings balance should never be negative,
    so any negative values are replaced with NaN.
    """
    # negative credit history
    neg_ch = df["credit_history_months"] < 0
    n_neg_ch = neg_ch.sum()
    if n_neg_ch > 0:
        print(f"Found {n_neg_ch} negative credit_history_months -> setting to NaN")
        df.loc[neg_ch, "credit_history_months"] = np.nan
    
    # negative savings balance
    neg_sb = df["savings_balance"] < 0
    n_neg_sb = neg_sb.sum()
    if n_neg_sb > 0:
        print(f"Found {n_neg_sb} negative savings_balance -> setting to NaN")
        df.loc[neg_sb, "savings_balance"] = np.nan
    
    return df


def validate_emails(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flag invalid email addresses with a boolean column.
    
    Uses a regex to check basic email format. We only flag (not correct)
    invalid entries because we can't guess the intended address.
    """
    email_pattern = re.compile(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$")
    
    def is_valid_email(val):
        if pd.isna(val) or val == "":
            return False
        return bool(email_pattern.match(str(val)))
    
    df["email_valid"] = df["email"].apply(is_valid_email)
    
    invalid_count = (~df["email_valid"]).sum()
    print(f"Email validation: {invalid_count} invalid emails flagged")
    return df


def flag_missing_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a completeness score for each record.
    Core fields that should be present: app_id, full_name, email, ssn, 
    ip_address, gender, date_of_birth, zip_code, annual_income, 
    credit_history_months, debt_to_income, savings_balance, loan_approved
    """
    core_fields = [
        "full_name", "email", "ssn", "ip_address", "gender",
        "date_of_birth", "zip_code", "annual_income",
        "credit_history_months", "debt_to_income", "savings_balance",
        "loan_approved"
    ]
    
    # count how many core fields are non-null and non-empty
    def count_complete(row):
        complete = 0
        for field in core_fields:
            val = row.get(field)
            if pd.notna(val) and val != "":
                complete += 1
        return complete
    
    df["completeness_score"] = df.apply(count_complete, axis=1)
    df["completeness_pct"] = (df["completeness_score"] / len(core_fields) * 100).round(1)
    
    incomplete = (df["completeness_pct"] < 100).sum()
    print(f"Completeness: {incomplete} records have missing core fields")
    return df


def flag_ssn_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flag SSNs that appear on more than one record.
    
    This catches both true duplicates and potential data-entry errors
    where different applicants were assigned the same SSN.
    """
    # only check non-null SSNs
    ssn_valid = df[df["ssn"].notna() & (df["ssn"] != "")]
    ssn_counts = ssn_valid["ssn"].value_counts()
    dup_ssns = ssn_counts[ssn_counts > 1].index.tolist()
    
    df["ssn_duplicate_flag"] = df["ssn"].isin(dup_ssns)
    
    if dup_ssns:
        print(f"Flagged {len(dup_ssns)} SSNs appearing on multiple different applicants")
    return df


def clean_pipeline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run the full cleaning pipeline in order.
    Each step prints what it did so we have a log.
    """
    print("=" * 50)
    print("STARTING DATA CLEANING PIPELINE")
    print("=" * 50)
    
    df = remove_duplicates(df)
    df = standardize_gender(df)
    df = normalize_dates(df)
    df = fix_income_types(df)
    df = fix_invalid_values(df)
    df = validate_emails(df)
    df = flag_missing_fields(df)
    df = flag_ssn_duplicates(df)
    
    print("=" * 50)
    print(f"CLEANING COMPLETE — {len(df)} records")
    print("=" * 50)
    return df


if __name__ == "__main__":
    from data_loader import load_and_flatten
    
    df = load_and_flatten("data/raw_credit_applications.json")
    df_clean = clean_pipeline(df)
    
    # save cleaned data
    df_clean.to_csv("data/cleaned_credit_applications.csv", index=False)
    print("\nSaved cleaned data to data/cleaned_credit_applications.csv")
