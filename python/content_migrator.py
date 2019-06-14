import os
import logging
from aol_helper import ArcGISOnlineHelper
from migration_helper import MigrationHelper
from settings import BX_CONTENTMIGRATION_AGS_PORTAL_LOGIN, BX_CONTENTMIGRATION_AGS_PORTAL_PASSWORD, \
    BX_DATASCIENCE_AGS_PORTAL_LOGIN, BX_DATASCIENCE_AGS_PORTAL_PASSWORD

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Note: username is case sensitive
# ags_helper = ArcGISOnlineHelper("Ujjwal.Narayan_bx_data_science", "Mx1Px4urc")
# user = ags_helper.gis.users.get("Ujjwal.Narayan_bx_data_science")

bx_cm_ags_helper = ArcGISOnlineHelper("https://bx-cm.maps.arcgis.com",
                                      BX_CONTENTMIGRATION_AGS_PORTAL_LOGIN,
                                      BX_CONTENTMIGRATION_AGS_PORTAL_PASSWORD)

bx_ds_ags_helper = ArcGISOnlineHelper("https://bx-data-science.maps.arcgis.com",
                                      BX_DATASCIENCE_AGS_PORTAL_LOGIN,
                                      BX_DATASCIENCE_AGS_PORTAL_PASSWORD)

bx_ds_migration_helper = MigrationHelper(bx_ds_ags_helper, 'bx_ds', logger)
bx_cm_migration_helper = MigrationHelper(bx_cm_ags_helper, 'bx_cm', logger)


# backing up api objects to local disk so i don't keep hitting api all the time
def backup_portals():
    bx_ds_migration_helper.backup_users()
    bx_ds_migration_helper.backup_groups()
    # backup for all users, for now only 'me'
    bx_ds_migration_helper.backup_user_content('me')

    bx_cm_migration_helper.backup_users()
    bx_cm_migration_helper.backup_groups()
    bx_cm_migration_helper.backup_user_content('me')


backup_portals()

# generate email basesd match between source and target portal users
bx_ds_migration_helper.match_users(bx_cm_migration_helper)

# migrate source groups to target
bx_ds_migration_helper.migrate_groups(target_portal=bx_cm_ags_helper.gis)

