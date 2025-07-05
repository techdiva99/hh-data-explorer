import pandas as pd
import os
import json
from collections import defaultdict
import networkx as nx
import matplotlib.pyplot as plt

# Paths
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
data_dir = os.path.join(project_root, 'data/processed')
input_csv = os.path.join(data_dir, 'new_final_master_provider.csv')
output_json = os.path.join(data_dir, 'provider_networks.json')

# Load data
df = pd.read_csv(input_csv, dtype=str)

# Fill NaNs with empty string for grouping
for col in ['ASSOCIATE ID', 'ORGANIZATION NAME', 'DOING BUSINESS AS NAME', 'NPI', 'CCN', 'PRACTICE LOCATION TYPE']:
    if col in df.columns:
        df[col] = df[col].fillna('')

# Build organization-level hierarchy
groups = df.groupby(['ASSOCIATE ID', 'ORGANIZATION NAME', 'DOING BUSINESS AS NAME'])
network = {"organizations": []}

for (associate_id, org_name, dba_name), group in groups:
    org_entry = {
        "associate_id": associate_id,
        "organization_name": org_name,
        "doing_business_as": dba_name,
        "npis": []
    }
    npi_groups = group.groupby('NPI')
    for npi, npi_group in npi_groups:
        npi_entry = {
            "npi": npi,
            "ccns": list(npi_group['CCN'].unique()),
            "practice_locations": []
        }
        for _, row in npi_group.iterrows():
            loc = {
                "type": row.get('PRACTICE LOCATION TYPE', ''),
                "address": row.get('ADDRESS LINE 1', ''),
                "city": row.get('CITY', ''),
                "state": row.get('STATE', ''),
                "zip": row.get('ZIP CODE', ''),
                "ccn": row.get('CCN', ''),
                "other_fields": {k: row[k] for k in row.index if k not in ['ASSOCIATE ID', 'ORGANIZATION NAME', 'DOING BUSINESS AS NAME', 'NPI', 'CCN', 'PRACTICE LOCATION TYPE', 'ADDRESS LINE 1', 'CITY', 'STATE', 'ZIP CODE']}
            }
            npi_entry["practice_locations"].append(loc)
        org_entry["npis"].append(npi_entry)
    network["organizations"].append(org_entry)

# Save as JSON
with open(output_json, 'w') as f:
    json.dump(network, f, indent=2)

# --- NetworkX Visualization and Summary ---
G = nx.Graph()

# Add nodes and edges: Organization <-> NPI <-> CCN
for org in network["organizations"]:
    org_label = f"ORG: {org['organization_name']}\nAssocID: {org['associate_id']}"
    G.add_node(org_label, type='organization')
    for npi_entry in org['npis']:
        npi_label = f"NPI: {npi_entry['npi']}"
        G.add_node(npi_label, type='npi')
        G.add_edge(org_label, npi_label)
        for ccn in npi_entry['ccns']:
            ccn_label = f"CCN: {ccn}"
            G.add_node(ccn_label, type='ccn')
            G.add_edge(npi_label, ccn_label)

# Draw a small subgraph for visualization (largest org)
org_sizes = [(org['organization_name'], len(org['npis'])) for org in network['organizations']]
if org_sizes:
    largest_org, max_npis = max(org_sizes, key=lambda x: x[1])
    print(f"Largest organization: {largest_org} with {max_npis} NPIs")
    # Subgraph for largest org
    largest_org_label = [n for n in G.nodes if n.startswith(f"ORG: {largest_org}")][0]
    nodes_to_draw = set([largest_org_label])
    nodes_to_draw.update(nx.descendants(G, largest_org_label))
    subG = G.subgraph(nodes_to_draw)
    plt.figure(figsize=(12, 8))
    pos = nx.spring_layout(subG, seed=42)
    node_colors = [
        'lightblue' if subG.nodes[n]['type'] == 'organization' else
        'orange' if subG.nodes[n]['type'] == 'npi' else
        'lightgreen' for n in subG.nodes
    ]
    nx.draw(subG, pos, with_labels=True, node_color=node_colors, font_size=8, node_size=1200)
    plt.title(f"Provider Network for Largest Organization: {largest_org}")
    plt.tight_layout()
    plt.savefig(os.path.join(data_dir, 'largest_org_network.png'))
    print(f"Network graph for largest organization saved to largest_org_network.png")


# Summary stats
num_orgs = len(network['organizations'])
num_npis = sum(len(org['npis']) for org in network['organizations'])
num_ccns = sum(len(npi['ccns']) for org in network['organizations'] for npi in org['npis'])
num_branches = sum(
    sum(1 for loc in npi['practice_locations'] if loc['type'].upper() == 'HHA BRANCH')
    for org in network['organizations'] for npi in org['npis']
)
print(f"Total organizations: {num_orgs}")
print(f"Total NPIs: {num_npis}")
print(f"Total CCNs: {num_ccns}")
print(f"Total HHA BRANCH locations: {num_branches}")

# Associate ID analysis
associate_ids = [org['associate_id'] for org in network['organizations'] if org['associate_id']]
from collections import Counter
aid_counts = Counter(associate_ids)
print(f"\nUnique ASSOCIATE IDs: {len(aid_counts)}")
print("Top 10 ASSOCIATE IDs by organization count:")
for aid, count in aid_counts.most_common(10):
    org_names = [org['organization_name'] for org in network['organizations'] if org['associate_id'] == aid]
    print(f"  ASSOCIATE ID: {aid} | Orgs: {count} | Org Names: {', '.join(set(org_names))}")
