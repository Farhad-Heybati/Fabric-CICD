# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "00dad5e5-d945-4e22-8ba4-49fbc9845d98",
# META       "default_lakehouse_name": "demo_databricks_v4",
# META       "default_lakehouse_workspace_id": "9d662f4d-ad03-4c16-82a4-8db29b422812",
# META       "known_lakehouses": [
# META         {
# META           "id": "00dad5e5-d945-4e22-8ba4-49fbc9845d98"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************


%pip install semantic-link-sempy
import sempy.fabric as fabric
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fabric.list_datasets()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Setup RLS at Semantic model

# CELL ********************

import json
from sempy.fabric import execute_tmsl  # keep your existing import if different

database_name = "poc-aas-direct-lake-adb-v5"
role_name = "RLS_Custmers_Python"

tmsl = {
  "createOrReplace": {
    "object": {
      "database": database_name,
      "role": role_name
    },
    "role": {
      "name": role_name,
      "modelPermission": "read",
      "members": [
        {
          "memberName": "RLS_User@MngEnvMCAP770244.onmicrosoft.com",
          "identityProvider": "AzureAD",
          "memberId": "49ea9dc1-b87e-4bf6-bcec-3bb544f4a17a"
        }
      ],
      "tablePermissions": [
        {
          "name": "customers_table",
          "filterExpression": "[CustomerKey] < 40"
        }
      ]
    }
  }
}

print(json.dumps(tmsl, indent=2))

execute_tmsl(
  script=tmsl,  # or json.dumps(tmsl) depending on your wrapper
  workspace="Databricks-Gold-Sales-Customers-Products"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import requests

# ---------------------------
# Inputs (edit these)
# ---------------------------
WORKSPACE_ID = "9d662f4d-ad03-4c16-82a4-8db29b422812"
ITEM_ID      = "14a2a448-8899-4e51-993d-5bb03744c08c"             # Lakehouse / Warehouse / Mirrored DB item id
ROLE_NAME    = "python_reader"
USER_UPN     = "RLS_User@MngEnvMCAP770244.onmicrosoft.com"
TABLE_PATH   = "Tables/customers_table"  # OneLake security path to the table
DRY_RUN      = False                     # set True to validate without applying

# ---------------------------
# Auth (Fabric notebook)
# ---------------------------
token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# ---------------------------
# Build payload for Create/Update Roles (batch)
# ---------------------------
payload = {
    "value": [
        {
            "name": ROLE_NAME,
            "decisionRules": [
                {
                    "effect": "Permit",
                    "permission": [
                        {
                            "attributeName": "Path",
                            "attributeValueIncludedIn": [TABLE_PATH]
                        },
                        {
                            "attributeName": "Action",
                            "attributeValueIncludedIn": ["Read"]
                        }
                    ]
                }
            ],
            # Assign the user to the role
            "members": {
                # The API supports Microsoft Entra identities (users, service principals, managed identities). [1](https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-data-access-security/create-or-update-data-access-roles)
                # The shape below is consistent with the roles API examples showing members under "members".
                "microsoftEntraMembers": [
                    {
                        "memberName": "RLS_User@MngEnvMCAP770244.onmicrosoft.com"
                    }
                ]
            }
        }
    ]
}

# ---------------------------
# Call the API
# ---------------------------
url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{ITEM_ID}/dataAccessRoles"
if DRY_RUN:
    url += "?dryRun=true"  # supported by the batch API [1](https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-data-access-security/create-or-update-data-access-roles)

print("Request URL:", url)
print("Payload:\n", json.dumps(payload, indent=2))

resp = requests.put(url, headers=headers, data=json.dumps(payload))

print("Status:", resp.status_code)
print("Response headers:", dict(resp.headers))
print("Response body:", resp.text)

resp.raise_for_status()
print(f"✅ Role '{ROLE_NAME}' created/updated successfully.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import requests

# ====== Inputs ======
WORKSPACE_ID = "9d662f4d-ad03-4c16-82a4-8db29b422812"
ITEM_ID      = "14a2a448-8899-4e51-993d-5bb03744c08c"    # lakehouse/warehouse item id
ROLE_NAME    = "pythonreader"

# ====== SPECIFIC MEMBER (Entra) ======
TENANT_ID = "867c7568-d5f5-4945-8db3-b9092ac63bac"
USER_OBJECT_ID = "49ea9dc1-b87e-4bf6-bcec-3bb544f4a17a"  # Entra Object ID for RLS_User@MngEnvMCAP770244.onmicrosoft.com

# OneLake table path predicates typically look like "/Tables/<tableName>"
# (Leading slash is used in the Learn examples for tablePath under constraints)
TABLE_PATH   = "/Tables/poc_aas/customers_table"

# Membership via item permissions (must be ARRAY + sourcePath required)
# Learn sample uses ["ReadAll"] — you can try ["ReadAll"] for broad read access.
ITEM_ACCESS  = ["ReadAll"]  # must be a JSON array, not "Read"

# ====== Auth (Fabric notebook) ======
token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

source_path = f"{WORKSPACE_ID}/{ITEM_ID}"

# ====== Payload must match Learn schema ======
payload = {
    "value": [
        {
            "name": ROLE_NAME,
            "decisionRules": [
                {
                    "effect": "Permit",
                    # IMPORTANT: permission is an ARRAY of AttributePredicate objects
                    "permission": [
                        {"attributeName": "Path",   "attributeValueIncludedIn": [TABLE_PATH]},
                        {"attributeName": "Action", "attributeValueIncludedIn": ["Read"]}
                    ]
                }
            ],
           "members": {
        "microsoftEntraMembers": [
          {
            "tenantId": TENANT_ID,
            "objectId": USER_OBJECT_ID,
            "objectType": "User"
          }
        ]
            }
        }
    ]
}

url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{ITEM_ID}/dataAccessRoles"

print("Request payload:\n", json.dumps(payload, indent=2))
resp = requests.put(url, headers=headers, data=json.dumps(payload))
print("Status:", resp.status_code)
print("Body:", resp.text)
resp.raise_for_status()
print("✅ Role created/updated.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
import requests

# ====== REQUIRED IDS ======
WORKSPACE_ID = "<your-workspace-guid>"
ITEM_ID      = "<your-lakehouse-guid>"  # the secured item (lakehouse/warehouse) id

# ====== ROLE SETTINGS ======
ROLE_NAME = "PythonReader"  # must start with a letter and contain only letters/numbers (no underscore)
TABLE_PATH = "/Tables/customers_table"  # use the OneLake path to the table

# ====== SPECIFIC MEMBER (Entra) ======
TENANT_ID = "<your-tenant-guid>"
USER_OBJECT_ID = "<user-object-guid>"  # Entra Object ID for RLS_User@MngEnvMCAP770244.onmicrosoft.com

# ====== AUTH (Fabric notebook) ======
token = mssparkutils.credentials.getToken("https://api.fabric.microsoft.com")
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

url = f"https://api.fabric.microsoft.com/v1/workspaces/{WORKSPACE_ID}/items/{ITEM_ID}/dataAccessRoles?dryRun=false"

payload = {
  "value": [
    {
      "name": ROLE_NAME,
      "decisionRules": [
        {
          "effect": "Permit",
          "permission": [
            {
              "attributeName": "Path",
              "attributeValueIncludedIn": [ TABLE_PATH ]
            },
            {
              "attributeName": "Action",
              "attributeValueIncludedIn": [ "Read" ]
            }
          ]
        }
      ],
      "members": {
        "microsoftEntraMembers": [
          {
            "tenantId": TENANT_ID,
            "objectId": USER_OBJECT_ID,
            "objectType": "User"
          }
        ]
      }
    }
  ]
}

print("Request payload:\n", json.dumps(payload, indent=2))

resp = requests.put(url, headers=headers, data=json.dumps(payload))
print("Status:", resp.status_code)
print("Body:\n", resp.text)
resp.raise_for_status()

print("✅ Role created/updated with specific Entra member")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
