[
  {
    "description": "Data Product Id",
    "mode": "NULLABLE",
    "name": "dp_id",
    "type": "STRING"
  },
  {
    "descriptions": "Unique ID for the Data Contract. This can be used to reference this contract in other data product's inputs port.",
    "mode": "NULLABLE",
    "name": "dpc_id",
    "type": "STRING"
  },
  {
    "mode": "REQUIRED",
    "name": "date",
    "type": "DATE"
  },
  {
    "description": "List of users",
    "mode": "REPEATED",
    "name": "users",
    "type": "STRING"
  },
  {
    "description": "List of Looker users",
    "mode": "REPEATED",
    "name": "looker_users",
    "type": "STRING"
  },
  {
    "description": "List of Service Accounts",
    "mode": "REPEATED",
    "name": "sas",
    "type": "STRING"
  },
 {
   "description": "Total Number of service accounts",
   "mode": "NULLABLE",
   "name": "total_sas",
   "type": "INTEGER"
 },
  {
    "description": "Total Number of users",
    "mode": "NULLABLE",
    "name": "total_users",
    "type": "INTEGER"
  },
  {
    "description": "Total Number of Looker users",
    "mode": "NULLABLE",
    "name": "total_looker_users",
    "type": "INTEGER"
  }
]
