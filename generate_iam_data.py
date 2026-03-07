import pandas as pd
import numpy as np
from faker import Faker
import uuid
import random
import os

# Initialize Faker
fake = Faker()
Faker.seed(42)
np.random.seed(42)

# Configuration / Scale
NUM_IDENTITIES = 50000
NUM_APPS = 1000
NUM_ENTITLEMENTS = 10000
ANOMALY_RATE = 0.03  # 3%

OUTPUT_DIR = "iam_dataset"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Starting IAM Data Generation...")

# ==========================================
# Step A: Base Dictionaries (Independent Variables)
# ==========================================

print("Step A: Generating Base Entities...")

# 1. Departments and Job Titles
departments = [
    'Engineering', 'Finance', 'HR', 'Sales', 'Marketing', 
    'IT', 'Legal', 'Operations', 'Product', 'Customer Support'
]

job_titles_map = {
    'Engineering': ['Software Engineer', 'DevOps Engineer', 'QA Engineer', 'Engineering Manager', 'Architect'],
    'Finance': ['Financial Analyst', 'Accountant', 'Controller', 'Finance Manager', 'Auditor'],
    'HR': ['HR Specialist', 'Recruiter', 'HR Manager', 'Benefits Coordinator', 'HRBP'],
    'Sales': ['Sales Representative', 'Account Executive', 'Sales Manager', 'Sales Ops', 'SDR'],
    'Marketing': ['Marketing Specialist', 'Content Writer', 'SEO Specialist', 'Marketing Manager', 'Designer'],
    'IT': ['SysAdmin', 'Network Engineer', 'Help Desk', 'IT Manager', 'Security Analyst'],
    'Legal': ['Legal Counsel', 'Paralegal', 'Compliance Officer', 'General Counsel', 'Contract Manager'],
    'Operations': ['Ops Manager', 'Logistics Coordinator', 'Project Manager', 'Analyst', 'Director of Ops'],
    'Product': ['Product Manager', 'Product Owner', 'Business Analyst', 'UX Designer', 'Head of Product'],
    'Customer Support': ['Support Agent', 'Support Lead', 'CS Manager', 'Technical Support', 'Success Manager']
}

# 2. Generate Identities
print("  - Generating Identities...")
identities_data = {
    'identity_id': [str(uuid.uuid4()) for _ in range(NUM_IDENTITIES)],
    'identity_type': np.random.choice(['Human', 'Service Account'], NUM_IDENTITIES, p=[0.95, 0.05]),
    'status': np.random.choice(['Active', 'Inactive', 'Leave'], NUM_IDENTITIES, p=[0.90, 0.05, 0.05]),
    'location': [fake.city() for _ in range(NUM_IDENTITIES)],
    'cost_center': [fake.bothify(text='CC-####') for _ in range(NUM_IDENTITIES)]
}

# Assign Departments and Titles
dept_choices = np.random.choice(departments, NUM_IDENTITIES)
identities_data['department'] = dept_choices

# Assign titles based on department
titles = []
for dept in dept_choices:
    titles.append(np.random.choice(job_titles_map[dept]))
identities_data['job_title'] = titles

df_identities = pd.DataFrame(identities_data)

# Assign Managers (Self-join simulation)
potential_managers = df_identities[df_identities['job_title'].str.contains('Manager|Director|Head|Lead')]['identity_id'].tolist()
if not potential_managers:
    potential_managers = df_identities['identity_id'].sample(int(NUM_IDENTITIES * 0.1)).tolist()

# Fix for ValueError: 'a' and 'p' must have same size
# Instead of adding None to the list and trying to balance probabilities,
# we will first assign managers, then randomly set some to None.
df_identities['manager_id'] = np.random.choice(potential_managers, NUM_IDENTITIES)

# Randomly set 10% of managers to None (e.g., top-level execs or data gaps)
mask_no_manager = np.random.rand(NUM_IDENTITIES) < 0.1
df_identities.loc[mask_no_manager, 'manager_id'] = None

# Ensure no self-management
df_identities.loc[df_identities['identity_id'] == df_identities['manager_id'], 'manager_id'] = None

# 3. Generate Applications
print("  - Generating Applications...")
app_ids = [str(uuid.uuid4()) for _ in range(NUM_APPS)]
df_apps = pd.DataFrame({
    'app_id': app_ids,
    'app_name': [f"App_{fake.word().capitalize()}_{i}" for i in range(NUM_APPS)],
    'business_criticality': np.random.choice(['Low', 'Medium', 'High', 'Critical'], NUM_APPS),
    'app_owner_id': np.random.choice(df_identities['identity_id'], NUM_APPS)
})

# 4. Generate Resources (1-3 per App)
print("  - Generating Resources...")
resources = []
for app_id in df_apps['app_id']:
    num_res = np.random.randint(1, 4)
    for _ in range(num_res):
        resources.append({
            'resource_id': str(uuid.uuid4()),
            'app_id': app_id,
            'iga_source_name': f"Source_{fake.word()}",
            'connection_type': np.random.choice(['Direct', 'JDBC', 'API', 'FlatFile'])
        })
df_resources = pd.DataFrame(resources)

# 5. Generate Entitlements
print("  - Generating Entitlements...")
resource_ids = df_resources['resource_id'].values
entitlements = []
for _ in range(NUM_ENTITLEMENTS):
    entitlements.append({
        'entitlement_id': str(uuid.uuid4()),
        'resource_id': np.random.choice(resource_ids),
        'entitlement_name': f"Ent_{fake.word().upper()}_{np.random.randint(1000, 9999)}",
        'is_requestable': np.random.choice([True, False]),
        'risk_level': np.random.choice(['Low', 'Medium', 'High'], p=[0.6, 0.3, 0.1])
    })
df_entitlements = pd.DataFrame(entitlements)

# ==========================================
# Step B: Ground Truth Rules / Clusters
# ==========================================
print("Step B: Defining Access Rules (Clusters)...")

# Map (Department, Job Title) -> Set of Entitlements
unique_roles = df_identities[['department', 'job_title']].drop_duplicates()
role_entitlement_map = {}

# Pre-fetch entitlement IDs
all_ent_ids = df_entitlements['entitlement_id'].values

for idx, row in unique_roles.iterrows():
    dept = row['department']
    title = row['job_title']
    key = (dept, title)
    
    # Determine number of entitlements for this role
    num_ents = np.random.randint(5, 21)
    
    # Select entitlements
    assigned_ents = np.random.choice(all_ent_ids, num_ents, replace=False)
    role_entitlement_map[key] = assigned_ents

print(f"  - Defined access rules for {len(role_entitlement_map)} unique roles.")

# ==========================================
# Step C: Relational Mapping (Assigning Access)
# ==========================================
print("Step C: Assigning Access based on Rules...")

# Create DataFrame from the map
role_map_list = []
for (dept, title), ent_ids in role_entitlement_map.items():
    for ent_id in ent_ids:
        role_map_list.append({'department': dept, 'job_title': title, 'entitlement_id': ent_id})

df_role_rules = pd.DataFrame(role_map_list)

# Merge Identities with Rules
print("  - Merging Identities with Access Rules...")
df_identity_access = df_identities.merge(df_role_rules, on=['department', 'job_title'], how='inner')

# Bring in resource_id
df_identity_access = df_identity_access.merge(df_entitlements[['entitlement_id', 'resource_id']], on='entitlement_id')

# Generate Accounts
print("  - Generating Accounts...")
df_accounts_needed = df_identity_access[['identity_id', 'resource_id']].drop_duplicates()
df_accounts_needed['account_id'] = [str(uuid.uuid4()) for _ in range(len(df_accounts_needed))]
df_accounts_needed['account_name'] = df_accounts_needed.apply(lambda x: f"acc_{x['identity_id'][:8]}", axis=1)
df_accounts_needed['is_privileged'] = False
df_accounts_needed['status'] = 'Active'

# Merge account_id back to the access list
df_identity_access = df_identity_access.merge(df_accounts_needed, on=['identity_id', 'resource_id'])

# Prepare Account_Entitlement table
df_account_entitlement = df_identity_access[['account_id', 'entitlement_id']].copy()
df_account_entitlement['grant_date'] = [fake.date_between(start_date='-2y', end_date='today') for _ in range(len(df_account_entitlement))]
df_account_entitlement['assignment_type'] = 'Birthright'

# ==========================================
# Step D: Inject Toxic Noise / Anomalies
# ==========================================
print("Step D: Injecting Anomalies...")

# Select random identities to be anomalous
num_anomalies = int(NUM_IDENTITIES * ANOMALY_RATE)
anomaly_identities = np.random.choice(df_identities['identity_id'], num_anomalies, replace=False)

anomaly_records = []
for identity_id in anomaly_identities:
    # Assign 1-3 random extra entitlements
    num_extra = np.random.randint(1, 4)
    extra_ents = np.random.choice(all_ent_ids, num_extra)
    
    for ent_id in extra_ents:
        anomaly_records.append({
            'identity_id': identity_id,
            'entitlement_id': ent_id,
            'assignment_type': 'Adhoc_Anomaly'
        })

df_anomalies = pd.DataFrame(anomaly_records)

# Resolve resources and accounts for anomalies
df_anomalies = df_anomalies.merge(df_entitlements[['entitlement_id', 'resource_id']], on='entitlement_id')

# Check if account already exists
df_anomalies = df_anomalies.merge(df_accounts_needed, on=['identity_id', 'resource_id'], how='left', suffixes=('', '_exist'))

# Create new accounts if needed
new_accounts_mask = df_anomalies['account_id'].isna()
num_new_accs = new_accounts_mask.sum()

if num_new_accs > 0:
    new_account_ids = [str(uuid.uuid4()) for _ in range(num_new_accs)]
    df_anomalies.loc[new_accounts_mask, 'account_id'] = new_account_ids
    df_anomalies.loc[new_accounts_mask, 'account_name'] = df_anomalies.loc[new_accounts_mask].apply(lambda x: f"acc_{x['identity_id'][:8]}_anom", axis=1)
    df_anomalies.loc[new_accounts_mask, 'is_privileged'] = False
    df_anomalies.loc[new_accounts_mask, 'status'] = 'Active'
    
    # Append new accounts to master accounts list
    new_accounts_df = df_anomalies.loc[new_accounts_mask, ['identity_id', 'resource_id', 'account_id', 'account_name', 'is_privileged', 'status']].drop_duplicates()
    df_accounts_needed = pd.concat([df_accounts_needed, new_accounts_df], ignore_index=True)

# Prepare Anomaly Account_Entitlements
df_anomaly_entitlements = df_anomalies[['account_id', 'entitlement_id', 'assignment_type']].copy()
df_anomaly_entitlements['grant_date'] = [fake.date_between(start_date='-6m', end_date='today') for _ in range(len(df_anomaly_entitlements))]

# Combine Normal and Anomaly Entitlements
df_final_account_entitlements = pd.concat([df_account_entitlement, df_anomaly_entitlements], ignore_index=True)

# ==========================================
# Finalizing and Saving
# ==========================================
print("Saving Dataframes to Parquet...")

# Identities
df_identities.to_parquet(os.path.join(OUTPUT_DIR, "identities.parquet"))

# Applications
df_apps.to_parquet(os.path.join(OUTPUT_DIR, "applications.parquet"))

# Resources
df_resources.to_parquet(os.path.join(OUTPUT_DIR, "resources.parquet"))

# Accounts
df_accounts_needed.to_parquet(os.path.join(OUTPUT_DIR, "accounts.parquet"))

# Entitlements
df_entitlements.to_parquet(os.path.join(OUTPUT_DIR, "entitlements.parquet"))

# Account_Entitlements
df_final_account_entitlements.to_parquet(os.path.join(OUTPUT_DIR, "account_entitlements.parquet"))

# Entitlement Groups
print("  - Generating Entitlement Groups...")
num_groups = 500
df_ent_groups = pd.DataFrame({
    'ent_group_id': [str(uuid.uuid4()) for _ in range(num_groups)],
    'group_name': [f"Group_{fake.word()}_{i}" for i in range(num_groups)],
    'description': [fake.sentence() for _ in range(num_groups)],
    'owner_id': np.random.choice(df_identities['identity_id'], num_groups)
})
df_ent_groups.to_parquet(os.path.join(OUTPUT_DIR, "entitlement_groups.parquet"))

# Entitlement_Group_Assignment
print("  - Generating Entitlement Group Assignments...")
group_ids = df_ent_groups['ent_group_id'].values
num_assignments = 20000
assigned_ids = np.random.choice(df_identities['identity_id'], num_assignments)
assigned_groups = np.random.choice(group_ids, num_assignments)

df_ent_group_assignment = pd.DataFrame({
    'identity_id': assigned_ids,
    'ent_group_id': assigned_groups,
    'assignment_status': 'Active'
})
df_ent_group_assignment.to_parquet(os.path.join(OUTPUT_DIR, "entitlement_group_assignments.parquet"))

# Entitlement_Group_Relation
print("  - Generating Entitlement Group Relations...")
num_relations = 100
parents = np.random.choice(group_ids, num_relations)
children = np.random.choice(group_ids, num_relations)
mask = parents != children
df_ent_group_relation = pd.DataFrame({
    'parent_ent_group_id': parents[mask],
    'child_ent_group_id': children[mask]
})
df_ent_group_relation.to_parquet(os.path.join(OUTPUT_DIR, "entitlement_group_relations.parquet"))

print("Done! Dataset generated in 'iam_dataset/' folder.")
