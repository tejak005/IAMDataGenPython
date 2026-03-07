import pandas as pd
import os

# Configuration
DATA_DIR = "iam_dataset"

def load_data():
    print("Loading Dataframes...")
    try:
        identities = pd.read_parquet(os.path.join(DATA_DIR, "identities.parquet"))
        applications = pd.read_parquet(os.path.join(DATA_DIR, "applications.parquet"))
        resources = pd.read_parquet(os.path.join(DATA_DIR, "resources.parquet"))
        accounts = pd.read_parquet(os.path.join(DATA_DIR, "accounts.parquet"))
        entitlements = pd.read_parquet(os.path.join(DATA_DIR, "entitlements.parquet"))
        account_entitlements = pd.read_parquet(os.path.join(DATA_DIR, "account_entitlements.parquet"))
        print("All files loaded successfully.\n")
        return identities, applications, resources, accounts, entitlements, account_entitlements
    except FileNotFoundError as e:
        print(f"Error loading files: {e}")
        exit(1)

def check_referential_integrity(df_child, child_col, df_parent, parent_col, relationship_name):
    """
    Checks if all non-null values in df_child[child_col] exist in df_parent[parent_col].
    """
    # Filter out nulls in child column (if any)
    valid_children = df_child[df_child[child_col].notna()]
    
    # Check existence
    mask = valid_children[child_col].isin(df_parent[parent_col])
    orphans = valid_children[~mask]
    num_orphans = len(orphans)
    
    status = "PASS" if num_orphans == 0 else "FAIL"
    print(f"[{status}] {relationship_name}: {num_orphans} orphans found.")
    return num_orphans

def validate_iam_data():
    # 1. Load Data
    identities, applications, resources, accounts, entitlements, account_entitlements = load_data()
    
    print("=== 2. Standard Foreign Key Checks ===")
    
    # Managers: Identity.manager_id -> Identity.identity_id
    check_referential_integrity(identities, 'manager_id', identities, 'identity_id', "Managers (Identity -> Identity)")
    
    # Resources: Resource.app_id -> Application.app_id
    check_referential_integrity(resources, 'app_id', applications, 'app_id', "Resources -> Applications")
    
    # Accounts: Account.identity_id -> Identity.identity_id
    check_referential_integrity(accounts, 'identity_id', identities, 'identity_id', "Accounts -> Identities")
    
    # Accounts: Account.resource_id -> Resource.resource_id
    check_referential_integrity(accounts, 'resource_id', resources, 'resource_id', "Accounts -> Resources")
    
    # Entitlements: Entitlement.resource_id -> Resource.resource_id
    check_referential_integrity(entitlements, 'resource_id', resources, 'resource_id', "Entitlements -> Resources")
    
    # Account_Entitlements: AE.account_id -> Account.account_id
    check_referential_integrity(account_entitlements, 'account_id', accounts, 'account_id', "Account_Entitlements -> Accounts")
    
    # Account_Entitlements: AE.entitlement_id -> Entitlement.entitlement_id
    check_referential_integrity(account_entitlements, 'entitlement_id', entitlements, 'entitlement_id', "Account_Entitlements -> Entitlements")
    
    print("\n=== 3. Enterprise IAM Domain Logic Checks ===")
    
    # --- The Account-Entitlement-Resource Triangle ---
    # Verify that Account and Entitlement belong to the same Resource
    print("Checking Account-Entitlement-Resource Triangle...")
    
    # Join AE -> Account (to get resource_id)
    ae_merged = account_entitlements.merge(accounts[['account_id', 'resource_id']], on='account_id', how='left', suffixes=('', '_acc'))
    
    # Join AE -> Entitlement (to get resource_id)
    ae_merged = ae_merged.merge(entitlements[['entitlement_id', 'resource_id']], on='entitlement_id', how='left', suffixes=('_acc', '_ent'))
    
    # Check for mismatches
    mismatches = ae_merged[ae_merged['resource_id_acc'] != ae_merged['resource_id_ent']]
    num_mismatches = len(mismatches)
    status = "PASS" if num_mismatches == 0 else "FAIL"
    print(f"[{status}] Resource Triangle Check: {num_mismatches} violations found (Account Resource != Entitlement Resource).")
    
    # --- Duplicate Assignments ---
    print("Checking for Duplicate Assignments...")
    duplicates = account_entitlements.duplicated(subset=['account_id', 'entitlement_id'])
    num_duplicates = duplicates.sum()
    status = "PASS" if num_duplicates == 0 else "FAIL"
    print(f"[{status}] Duplicate Assignments: {num_duplicates} duplicate rows found.")
    
    # --- Active vs. Inactive Users ---
    print("Checking Active Entitlements for Inactive Users...")
    # Join AE -> Account -> Identity
    # We already have ae_merged with account info, let's join Identity status
    ae_identity = ae_merged.merge(accounts[['account_id', 'identity_id']], on='account_id', how='left')
    ae_identity = ae_identity.merge(identities[['identity_id', 'status']], on='identity_id', how='left')
    
    # Filter for Inactive/Leave status
    inactive_access = ae_identity[ae_identity['status'].isin(['Inactive', 'Leave'])]
    num_inactive_access = len(inactive_access)
    
    # This is not necessarily a "FAIL" in terms of data integrity, but a "WARNING" for business logic
    # However, the prompt asks to "Flag" them.
    status = "INFO" if num_inactive_access > 0 else "PASS"
    print(f"[{status}] Inactive User Access: {num_inactive_access} entitlements held by Inactive/Leave users.")

    print("\n=== Validation Complete ===")

if __name__ == "__main__":
    validate_iam_data()
