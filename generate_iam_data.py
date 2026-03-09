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
# NUM_ENTITLEMENTS dynamic
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

# Assign Managers
potential_managers = df_identities[df_identities['job_title'].str.contains('Manager|Director|Head|Lead')]['identity_id'].tolist()
if not potential_managers:
    potential_managers = df_identities['identity_id'].sample(int(NUM_IDENTITIES * 0.1)).tolist()

df_identities['manager_id'] = np.random.choice(potential_managers, NUM_IDENTITIES)
mask_no_manager = np.random.rand(NUM_IDENTITIES) < 0.1
df_identities.loc[mask_no_manager, 'manager_id'] = None
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

# 4. Generate Resources (Skewed + Types)
print("  - Generating Resources...")
resources = []
for app_id in df_apps['app_id']:
    # Determine App Type: Platform (AD/LDAP/AWS) vs Standard App
    # 5% Platforms (High Entitlement Count per Account), 95% Standard
    is_platform = np.random.random() < 0.05
    resource_type = 'Platform' if is_platform else 'Application'
    
    # Resource Count Distribution
    if is_platform:
        num_res = 1 # Platforms usually have 1 main resource (the directory)
    else:
        # Skewed distribution for apps
        rand_val = np.random.random()
        if rand_val < 0.60: num_res = 1
        elif rand_val < 0.85: num_res = 2
        elif rand_val < 0.95: num_res = np.random.randint(3, 6)
        else: num_res = np.random.randint(6, 16)

    for _ in range(num_res):
        resources.append({
            'resource_id': str(uuid.uuid4()),
            'app_id': app_id,
            'iga_source_name': f"Source_{fake.word()}",
            'connection_type': np.random.choice(['Direct', 'JDBC', 'API', 'FlatFile']),
            'resource_type': resource_type
        })
df_resources = pd.DataFrame(resources)

# 5. Generate Entitlements
print("  - Generating Entitlements...")
entitlements = []
resource_map = df_resources.set_index('resource_id')['resource_type'].to_dict()
resource_ids = df_resources['resource_id'].values

for res_id in resource_ids:
    res_type = resource_map[res_id]
    
    # Entitlement Count based on Resource Type
    if res_type == 'Platform':
        # Platforms have MANY entitlements (Groups, Roles)
        # 50 to 500 entitlements
        num_ents = np.random.randint(50, 501)
    else:
        # Standard Apps
        # 70% Simple (1-3), 25% Moderate (4-10), 5% Complex (11-30)
        rand_val = np.random.random()
        if rand_val < 0.70: num_ents = np.random.randint(1, 4)
        elif rand_val < 0.95: num_ents = np.random.randint(4, 11)
        else: num_ents = np.random.randint(11, 31)
        
    for _ in range(num_ents):
        entitlements.append({
            'entitlement_id': str(uuid.uuid4()),
            'resource_id': res_id,
            'entitlement_name': f"Ent_{fake.word().upper()}_{np.random.randint(1000, 9999)}",
            'is_requestable': np.random.choice([True, False]),
            'owner_id': np.random.choice(df_identities['identity_id'])
        })

df_entitlements = pd.DataFrame(entitlements)
print(f"  - Generated {len(df_entitlements)} entitlements.")

# ==========================================
# Step B: Entitlement Groups & Hierarchy (Real World Simulation)
# ==========================================
print("Step B: Generating Entitlement Groups & Hierarchy...")

# Structure:
# 1. Organization Root (1)
# 2. Departments (10) - Children of Root
# 3. Teams (Many) - Children of Departments
# 4. Projects (Many) - Ad-hoc, no strict parent or attached to Root

groups = []
relations = []

# 1. Root Group
root_group_id = str(uuid.uuid4())
groups.append({
    'ent_group_id': root_group_id,
    'group_name': 'All Employees',
    'description': 'Root organization group',
    'owner_id': np.random.choice(df_identities['identity_id']),
    'type': 'Organization'
})

# 2. Department Groups
dept_group_ids = {}
for dept in departments:
    g_id = str(uuid.uuid4())
    dept_group_ids[dept] = g_id
    groups.append({
        'ent_group_id': g_id,
        'group_name': f"Dept_{dept}",
        'description': f"All members of {dept}",
        'owner_id': np.random.choice(df_identities['identity_id']),
        'type': 'Department'
    })
    # Relation: Root -> Dept
    relations.append({'parent_ent_group_id': root_group_id, 'child_ent_group_id': g_id})

# 3. Team Groups (Sub-groups within Departments)
# Generate ~500 team groups distributed across departments
team_group_ids = []
for _ in range(500):
    dept = np.random.choice(departments)
    parent_id = dept_group_ids[dept]
    g_id = str(uuid.uuid4())
    team_group_ids.append(g_id)
    groups.append({
        'ent_group_id': g_id,
        'group_name': f"Team_{dept}_{fake.word().capitalize()}",
        'description': f"Team within {dept}",
        'owner_id': np.random.choice(df_identities['identity_id']),
        'type': 'Team'
    })
    # Relation: Dept -> Team
    relations.append({'parent_ent_group_id': parent_id, 'child_ent_group_id': g_id})

# 4. Project Groups (Ad-hoc)
# Generate ~200 project groups
project_group_ids = []
for _ in range(200):
    g_id = str(uuid.uuid4())
    project_group_ids.append(g_id)
    groups.append({
        'ent_group_id': g_id,
        'group_name': f"Project_{fake.word().capitalize()}",
        'description': "Cross-functional project team",
        'owner_id': np.random.choice(df_identities['identity_id']),
        'type': 'Project'
    })
    # No strict hierarchy for projects, or maybe Root -> Project
    if np.random.random() < 0.3:
        relations.append({'parent_ent_group_id': root_group_id, 'child_ent_group_id': g_id})

df_ent_groups = pd.DataFrame(groups)
df_ent_group_relation = pd.DataFrame(relations)

# ==========================================
# Step C: Group Assignments (Identity -> Group)
# ==========================================
print("Step C: Assigning Identities to Groups...")
assignments = []

# 1. All users -> Root Group
for uid in df_identities['identity_id']:
    assignments.append({'identity_id': uid, 'ent_group_id': root_group_id, 'assignment_status': 'Active'})

# 2. Users -> Dept Group
for idx, row in df_identities.iterrows():
    dept_gid = dept_group_ids[row['department']]
    assignments.append({'identity_id': row['identity_id'], 'ent_group_id': dept_gid, 'assignment_status': 'Active'})

# 3. Users -> Team Groups (Zipfian/Skewed)
# Most users in 1 team, some in 2, few in 3.
# Teams are specific to their department.
team_map = pd.DataFrame(groups)
team_map = team_map[team_map['type'] == 'Team']
# We need to know which team belongs to which dept to assign correctly
# Re-map team_id to dept
team_to_dept = {}
for rel in relations:
    # Find relations where parent is a Dept Group
    parent = rel['parent_ent_group_id']
    child = rel['child_ent_group_id']
    # Check if parent is in dept_group_ids values
    dept_name = next((k for k, v in dept_group_ids.items() if v == parent), None)
    if dept_name:
        team_to_dept[child] = dept_name

# Assign
for idx, row in df_identities.iterrows():
    dept = row['department']
    # Find teams in this dept
    possible_teams = [tid for tid, d in team_to_dept.items() if d == dept]
    if possible_teams:
        # Assign to 1-2 teams
        num_teams = np.random.choice([1, 2], p=[0.8, 0.2])
        selected_teams = np.random.choice(possible_teams, min(len(possible_teams), num_teams), replace=False)
        for tid in selected_teams:
            assignments.append({'identity_id': row['identity_id'], 'ent_group_id': tid, 'assignment_status': 'Active'})

# 4. Users -> Project Groups (Ad-hoc, Zipfian)
# Some projects are huge, some small.
# We assign users to projects.
# 20% of users are in projects.
project_users = df_identities['identity_id'].sample(frac=0.2)
for uid in project_users:
    # Pick a project. Prefer "popular" projects (Zipfian simulation via random weights)
    # Simple way: shuffle project IDs, pick one.
    # To make some large: assign weights to projects.
    proj_weights = np.random.pareto(a=2, size=len(project_group_ids))
    proj_weights /= proj_weights.sum()
    pid = np.random.choice(project_group_ids, p=proj_weights)
    assignments.append({'identity_id': uid, 'ent_group_id': pid, 'assignment_status': 'Active'})

df_ent_group_assignment = pd.DataFrame(assignments)

# ==========================================
# Step D: Group -> Entitlement Mapping
# ==========================================
print("Step D: Mapping Groups to Entitlements...")
# Assign entitlements to groups to simulate "Roles" or "Access Profiles"
group_entitlements = []
all_ent_ids = df_entitlements['entitlement_id'].values

# 1. Root Group -> Basic Birthright (e.g., Intranet access)
# Pick 2-3 random entitlements
basic_ents = np.random.choice(all_ent_ids, 3, replace=False)
for eid in basic_ents:
    group_entitlements.append({'ent_group_id': root_group_id, 'entitlement_id': eid})

# 2. Dept Groups -> Dept specific tools
for dept, gid in dept_group_ids.items():
    # Pick 5-10 entitlements
    ents = np.random.choice(all_ent_ids, np.random.randint(5, 11))
    for eid in ents:
        group_entitlements.append({'ent_group_id': gid, 'entitlement_id': eid})

# 3. Team Groups -> Specific access
for tid in team_group_ids:
    # Pick 2-5 entitlements
    ents = np.random.choice(all_ent_ids, np.random.randint(2, 6))
    for eid in ents:
        group_entitlements.append({'ent_group_id': tid, 'entitlement_id': eid})

df_group_entitlements = pd.DataFrame(group_entitlements)

# ==========================================
# Step E: Account Entitlements (Simulating Real World)
# ==========================================
print("Step E: Generating Account Entitlements...")

# Strategy:
# 1. Platform Resources (AD/AWS): Accounts have MANY entitlements (reflecting group memberships).
# 2. App Resources: Accounts have FEW entitlements (1-2).

# Prepare helper dicts
ents_by_resource = df_entitlements.groupby('resource_id')['entitlement_id'].apply(list).to_dict()
res_type_map = df_resources.set_index('resource_id')['resource_type'].to_dict()
platform_res_ids = [rid for rid, rtype in res_type_map.items() if rtype == 'Platform']
app_res_ids = [rid for rid, rtype in res_type_map.items() if rtype == 'Application']

role_map_list = []
unique_roles = df_identities[['department', 'job_title']].drop_duplicates()

for idx, row in unique_roles.iterrows():
    dept = row['department']
    title = row['job_title']
    
    # 1. Assign 1 Platform Resource (e.g., AD Account)
    if platform_res_ids:
        plat_res = np.random.choice(platform_res_ids)
        avail_ents = ents_by_resource.get(plat_res, [])
        if avail_ents:
            # Assign 5-20 entitlements (Groups)
            num_ents = np.random.randint(5, 21)
            picked = np.random.choice(avail_ents, min(len(avail_ents), num_ents), replace=False)
            for eid in picked:
                role_map_list.append({'department': dept, 'job_title': title, 'entitlement_id': eid})

    # 2. Assign 3-8 Application Resources
    num_apps = np.random.randint(3, 9)
    selected_apps = np.random.choice(app_res_ids, min(len(app_res_ids), num_apps), replace=False)
    
    for app_res in selected_apps:
        avail_ents = ents_by_resource.get(app_res, [])
        if avail_ents:
            # Assign 1-2 entitlements (User, maybe Admin)
            # 90% chance of 1, 10% chance of 2
            num_ents = 1 if np.random.random() < 0.9 else 2
            picked = np.random.choice(avail_ents, min(len(avail_ents), num_ents), replace=False)
            for eid in picked:
                role_map_list.append({'department': dept, 'job_title': title, 'entitlement_id': eid})

df_role_rules = pd.DataFrame(role_map_list)

# Merge Identities with Rules
print("  - Merging Identities with Access Rules...")
df_identity_access = df_identities.merge(df_role_rules, on=['department', 'job_title'], how='inner')
df_identity_access = df_identity_access.merge(df_entitlements[['entitlement_id', 'resource_id']], on='entitlement_id')

# Generate Accounts
print("  - Generating Accounts...")
df_accounts_needed = df_identity_access[['identity_id', 'resource_id']].drop_duplicates()
df_accounts_needed['account_id'] = [str(uuid.uuid4()) for _ in range(len(df_accounts_needed))]
df_accounts_needed['account_name'] = df_accounts_needed.apply(lambda x: f"acc_{x['identity_id'][:8]}", axis=1)
df_accounts_needed['is_privileged'] = False
df_accounts_needed['status'] = 'Active'

# Merge account_id back
df_identity_access = df_identity_access.merge(df_accounts_needed, on=['identity_id', 'resource_id'])

# Prepare Account_Entitlement table
df_account_entitlement = df_identity_access[['identity_id', 'account_id', 'entitlement_id']].copy()
df_account_entitlement['grant_date'] = [fake.date_between(start_date='-2y', end_date='today') for _ in range(len(df_account_entitlement))]
df_account_entitlement['assignment_type'] = 'Birthright'

# ==========================================
# Step F: Anomalies
# ==========================================
print("Step F: Injecting Anomalies...")
# (Simplified Anomaly Logic to match new structure)
num_anomalies = int(NUM_IDENTITIES * ANOMALY_RATE)
anomaly_identities = np.random.choice(df_identities['identity_id'], num_anomalies, replace=False)
anomaly_records = []

for identity_id in anomaly_identities:
    # Pick random resource
    res_id = np.random.choice(resource_ids)
    avail_ents = ents_by_resource[res_id]
    if not avail_ents: continue
    
    # Pick 1 entitlement
    ent_id = np.random.choice(avail_ents)
    anomaly_records.append({
        'identity_id': identity_id,
        'entitlement_id': ent_id,
        'assignment_type': 'Adhoc_Anomaly'
    })

df_anomalies = pd.DataFrame(anomaly_records)
df_anomalies = df_anomalies.merge(df_entitlements[['entitlement_id', 'resource_id']], on='entitlement_id')
df_anomalies = df_anomalies.merge(df_accounts_needed, on=['identity_id', 'resource_id'], how='left', suffixes=('', '_exist'))

# New accounts for anomalies
new_accounts_mask = df_anomalies['account_id'].isna()
if new_accounts_mask.sum() > 0:
    new_ids = [str(uuid.uuid4()) for _ in range(new_accounts_mask.sum())]
    df_anomalies.loc[new_accounts_mask, 'account_id'] = new_ids
    df_anomalies.loc[new_accounts_mask, 'account_name'] = df_anomalies.loc[new_accounts_mask].apply(lambda x: f"acc_{x['identity_id'][:8]}_anom", axis=1)
    df_anomalies.loc[new_accounts_mask, 'is_privileged'] = False
    df_anomalies.loc[new_accounts_mask, 'status'] = 'Active'
    
    new_acc_df = df_anomalies.loc[new_accounts_mask, ['identity_id', 'resource_id', 'account_id', 'account_name', 'is_privileged', 'status']].drop_duplicates()
    df_accounts_needed = pd.concat([df_accounts_needed, new_acc_df], ignore_index=True)

df_anomaly_entitlements = df_anomalies[['identity_id', 'account_id', 'entitlement_id', 'assignment_type']].copy()
df_anomaly_entitlements['grant_date'] = [fake.date_between(start_date='-6m', end_date='today') for _ in range(len(df_anomaly_entitlements))]

df_final_account_entitlements = pd.concat([df_account_entitlement, df_anomaly_entitlements], ignore_index=True)

# ==========================================
# Saving
# ==========================================
print("Saving Dataframes to Parquet...")
df_identities.to_parquet(os.path.join(OUTPUT_DIR, "identities.parquet"))
df_apps.to_parquet(os.path.join(OUTPUT_DIR, "applications.parquet"))
df_resources.to_parquet(os.path.join(OUTPUT_DIR, "resources.parquet"))
df_accounts_needed.to_parquet(os.path.join(OUTPUT_DIR, "accounts.parquet"))
df_entitlements.to_parquet(os.path.join(OUTPUT_DIR, "entitlements.parquet"))
df_final_account_entitlements.to_parquet(os.path.join(OUTPUT_DIR, "account_entitlements.parquet"))
df_ent_groups.to_parquet(os.path.join(OUTPUT_DIR, "entitlement_groups.parquet"))
df_ent_group_assignment.to_parquet(os.path.join(OUTPUT_DIR, "entitlement_group_assignments.parquet"))
df_ent_group_relation.to_parquet(os.path.join(OUTPUT_DIR, "entitlement_group_relations.parquet"))
df_group_entitlements.to_parquet(os.path.join(OUTPUT_DIR, "group_entitlements.parquet"))

print("Done! Dataset generated in 'iam_dataset/' folder.")
