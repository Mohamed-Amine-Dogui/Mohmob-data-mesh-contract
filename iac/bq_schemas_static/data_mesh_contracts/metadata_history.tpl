[
    {
    "descriptions": "Data Product Id",
    "mode": "NULLABLE",
    "name": "dp_id",
    "type": "STRING"
    },
    {
    "descriptions": "Data Product Name",
    "mode": "NULLABLE",
    "name": "dp_name",
    "type": "STRING"
    },
    {
    "descriptions": "Data Domain / crew owning this Data Product.",
    "mode": "NULLABLE",
    "name": "dp_owner_dataDomain",
    "type": "STRING"
    },
    {
    "descriptions": "Link to team in Application Inventory (https://tech.tools.mobint.io/application-inventory)",
    "mode": "NULLABLE",
    "name": "dp_owner_contact",
    "type": "STRING"
    },
    {
    "descriptions": "The domain of Team/Crew with domain knowledge of the data product’s offerings. Ideally, they are the same as the “Data Product Owner”. The goal in our Data Mesh vision is to align these two, and deprecate this term.",
    "mode": "NULLABLE",
    "name": "da_owner_dataDomain",
    "type": "STRING"
    },
    {
    "descriptions": "The contact of Team/Crew with domain knowledge of the data product’s offerings. Ideally, they are the same as the “Data Product Owner”. The goal in our Data Mesh vision is to align these two, and deprecate this term.",
    "mode": "NULLABLE",
    "name": "da_owner_contact",
    "type": "STRING"
    },
    {
    "descriptions": "Status representing lifecycle of this data product. Possible values are: proposed, development, active, decommissioned",
    "mode": "NULLABLE",
    "name": "dp_maturity",
    "type": "STRING"
    },
    {
    "descriptions": "Unique ID for the Data Contract. This can be used to reference this contract in other data product's inputs port.",
    "mode": "NULLABLE",
    "name": "dpc_id",
    "type": "STRING"
    },
    {
    "descriptions": "Friendly name for this Data Contract",
    "mode": "NULLABLE",
    "name": "dpc_name",
    "type": "STRING"
    },
    {
    "descriptions": "Version of this data contract",
    "mode": "NULLABLE",
    "name": "dpc_version",
    "type": "STRING"
    },
    {
    "descriptions": "List of input ports for this Data Product's output port",
    "mode": "REPEATED",
    "name": "dpc_input_port",
    "type": "JSON"
    },
    {
    "descriptions": "Physical output port.[BigQueryStorage, KafkaStorage, ObjectStorage]",
    "mode": "NULLABLE",
    "name": "dpc_output_port",
    "type": "JSON"
    },
    {
    "descriptions": "List of quality checks for this output port",
    "mode": "REPEATED",
    "name": "dpc_quality",
    "type": "JSON"
    },
    {
    "mode": "NULLABLE",
    "name": "ingested_timestamp",
    "type": "TIMESTAMP",
    "description": "Date when the event happened."
    },
    {
    "mode": "REQUIRED",
    "name": "date",
    "type": "DATE"
    }
]
