#************************** MISC **********************************
DJANGO_SETTINGS_MODULE="app.settings.default"
LOG_LEVEL="INFO"
AG_GRID_LICENSE
ARM_TENANT_ID=
#USE_DATABRICKS_CLUSTER=True
DATABRICKS_BRONZE_LAYER="ussparc_bronze"
DATABRICKS_SILVER_LAYER="ussparc_silver"
DATABRICKS_GOLD_LAYER="ussparc_gold"
DATABRICKS_NIWA_GOLD_LAYER="niwa_gold"
WORKSPACE_PATH='/Workspace/Shared/USSPARC_Deployed/backend'

WEBAPP_SERVER_ENDPOINT='app-ussparc-dev-use2-01.azurewebsites.net/api'
AZURE_FRONTEND_VOLUME_STORAGE='bronze/volume/frontend'
USSPARC_BRONZE_VOLUME_ACC=dlsussparcduse202

BENCHMARKING_TOOL_URL='https://apps.powerapps.com/play/e/81c821fe-2426-498d-822e-82dac1a209ed/a/15c1ee39-1298-4c94-a670-aac13b54637e?tenantId=c7f6413d-1e73-45d2-b0da-a68713b515a7&hint=f72123d2-0af7-4ff4-964d-b5d442253c3d&sourcetime=1748874453173&source=portal'
#****************** CLIENT CREDS DEV **********************************

#****************** CLIENT CREDS QA **********************************

#******************* Databricks DEV **********************************
DATABRICKS_CATALOG="rmdad_grc_dev"
DATABRICKS_NIWA_CATALOG="niwa_dev"
DATABRICKS_NIWA_CATALOG_GOLD = "gold"
DATABRICKS_CLUSTER_ID="0110-142817-ps67uthz"
DATABRICKS_HOST="https://adb-7323138722438154.14.azuredatabricks.net"
DATABRICKS_SQL_WAREHOUSE_HTTP="/sql/1.0/warehouses/09fd8b4d11e0a242"
#******************* Databricks QA *************************************
# DATABRICKS_CLUSTER_ID=
# DATABRICKS_HOST="https://adb-3777840164948871.11.azuredatabricks.net"
# DATABRICKS_CATALOG="rmdad_grc_qa"
# DATABRICKS_NIWA_CATALOG="niwa_qa"
# DATABRICKS_LAKEHOUSE_CATALOG="datamgmnt_edw_qa"
#******************* Databricks REL *************************************
# DATABRICKS_CLUSTER_ID=
# DATABRICKS_HOST=
# DATABRICKS_CATALOG="rmdad_grc_rel"
# DATABRICKS_NIWA_CATALOG="niwa_rel"
# DATABRICKS_LAKEHOUSE_CATALOG="datamgmnt_edw_rel"
# DATABRICKS_SQL_WAREHOUSE_HTTP="/sql/1.0/warehouses/23bb73485a330898"
#********************** POLARIS ********************************
POLARIS_SERVER=
POLARIS_WF_BASE_ENDPOINT='/graph-workflow-client/var/workflow/var_initialization/submit'
POLARIS_WF_SCENARIO_ENDPOINT='/graph-workflow-client/var/workflow/var_scenarios/submit'
POLARIS_API_TOKEN_ENDPOINT='https://gateway-polaris-tooling-dev.apps.ocp202.smbcgroup.com/s2sauth/api/token'
POLARIS_VOLUME_STORAGE='bronze/volume/polaris'
POLARIS_VALUATION_STORAGE='bronze/volume/polaris'
#*********************AZURE SQL**************************************
