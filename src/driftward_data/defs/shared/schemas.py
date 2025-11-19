"""BigQuery schemas for SBA loan data."""

# BigQuery schema for SBA 7(a) loans
# Using NULLABLE for all fields to handle data quality issues
SBA_7A_SCHEMA = [
    # Temporal & Program
    {"name": "AsOfDate", "type": "DATE", "mode": "NULLABLE"},
    {"name": "Program", "type": "STRING", "mode": "NULLABLE"},
    # Borrower
    {"name": "BorrName", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BorrStreet", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BorrCity", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BorrState", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BorrZip", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "LocationID", "type": "INTEGER", "mode": "NULLABLE"},
    # Lender
    {"name": "BankName", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BankFDICNumber", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "BankNCUANumber", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "BankStreet", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BankCity", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BankState", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BankZip", "type": "INTEGER", "mode": "NULLABLE"},
    # Loan Terms
    {"name": "GrossApproval", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "SBAGuaranteedApproval", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "ApprovalDate", "type": "DATE", "mode": "NULLABLE"},
    {"name": "ApprovalFY", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "FirstDisbursementDate", "type": "DATE", "mode": "NULLABLE"},
    {"name": "ProcessingMethod", "type": "STRING", "mode": "NULLABLE"},
    {"name": "Subprogram", "type": "STRING", "mode": "NULLABLE"},
    {"name": "InitialInterestRate", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "FixedorVariableInterestRate", "type": "STRING", "mode": "NULLABLE"},
    {"name": "TerminMonths", "type": "INTEGER", "mode": "NULLABLE"},
    # Business Classification
    {"name": "NAICSCode", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "NAICSDescription", "type": "STRING", "mode": "NULLABLE"},
    {"name": "FranchiseCode", "type": "STRING", "mode": "NULLABLE"},
    {"name": "FranchiseName", "type": "STRING", "mode": "NULLABLE"},
    # Project Location
    {"name": "ProjectCounty", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ProjectState", "type": "STRING", "mode": "NULLABLE"},
    {"name": "SBADistrictOffice", "type": "STRING", "mode": "NULLABLE"},
    {"name": "CongressionalDistrict", "type": "INTEGER", "mode": "NULLABLE"},
    # Business Details
    {"name": "BusinessType", "type": "STRING", "mode": "NULLABLE"},
    {"name": "BusinessAge", "type": "STRING", "mode": "NULLABLE"},
    # Loan Status
    {"name": "LoanStatus", "type": "STRING", "mode": "NULLABLE"},
    {"name": "PaidinFullDate", "type": "DATE", "mode": "NULLABLE"},
    {"name": "ChargeoffDate", "type": "DATE", "mode": "NULLABLE"},
    {"name": "GrossChargeoffAmount", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "RevolverStatus", "type": "INTEGER", "mode": "NULLABLE"},
    # Impact & Features
    {"name": "JobsSupported", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "CollateralInd", "type": "STRING", "mode": "NULLABLE"},
    {"name": "SoldSecondMarketInd", "type": "STRING", "mode": "NULLABLE"},
]
