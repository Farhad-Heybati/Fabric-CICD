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

# Welcome to your new notebook
# Type here in the cell editor to add code!



import json

tmsl = {
  "createOrReplace": {
    "object": {
      "database": "poc-aas-direct-lake-adb-v5",
      "role": "West US 3"
    },
    "role": {
      "name": "RLS_Custmers_Python",
      "modelPermission": "read",
      "members": [
        {
          "memberName": "RLS_User@MngEnvMCAP770244.onmicrosoft.com",
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

tmsl_text = json.dumps(tmsl)
print(tmsl_text)

fabric.execute_tmsl(tmsl, workspace="Databricks-Gold-Sales-Customers-Products")

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
