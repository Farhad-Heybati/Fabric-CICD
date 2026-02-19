[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared sales_table_v2 = let
    Source = Databricks.Catalogs("adb-2658039321675167.7.azuredatabricks.net", "/sql/1.0/warehouses/c95c601b9e925b18", [Catalog=null, Database=null, QueryTags=null, EnableAutomaticProxyDiscovery=null, Implementation=null]),
    demo_databricks_v3_Database = Source{[Name="demo_databricks_v3",Kind="Database"]}[Data],
    poc_aas_Schema = demo_databricks_v3_Database{[Name="poc_aas",Kind="Schema"]}[Data],
    sales_table_Table = poc_aas_Schema{[Name="sales_table",Kind="Table"]}[Data]
in
    sales_table_Table;
[BindToDefaultDestination = true]
shared products_table_v2 = let
    Source = Databricks.Catalogs("adb-2658039321675167.7.azuredatabricks.net", "/sql/1.0/warehouses/c95c601b9e925b18", [Catalog=null, Database=null, QueryTags=null, EnableAutomaticProxyDiscovery=null, Implementation=null]),
    demo_databricks_v3_Database = Source{[Name="demo_databricks_v3",Kind="Database"]}[Data],
    poc_aas_Schema = demo_databricks_v3_Database{[Name="poc_aas",Kind="Schema"]}[Data],
    products_table_Table = poc_aas_Schema{[Name="products_table",Kind="Table"]}[Data]
in
    products_table_Table;
[BindToDefaultDestination = true]
shared customers_table_v2 = let
    Source = Databricks.Catalogs("adb-2658039321675167.7.azuredatabricks.net", "/sql/1.0/warehouses/c95c601b9e925b18", [Catalog=null, Database=null, QueryTags=null, EnableAutomaticProxyDiscovery=null, Implementation=null]),
    demo_databricks_v3_Database = Source{[Name="demo_databricks_v3",Kind="Database"]}[Data],
    poc_aas_Schema = demo_databricks_v3_Database{[Name="poc_aas",Kind="Schema"]}[Data],
    customers_table_Table = poc_aas_Schema{[Name="customers_table",Kind="Table"]}[Data]
in
    customers_table_Table;
[BindToDefaultDestination = true]
shared DefaultDestination = Lakehouse.Contents([EnableFolding = false]){[workspaceId = "9d662f4d-ad03-4c16-82a4-8db29b422812"]}[Data]{[lakehouseId = "7718fd09-9ee5-4c40-9360-0fe37c130460"]}[Data];
