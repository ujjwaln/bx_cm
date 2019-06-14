import os
import jsonpickle
from tinydb import TinyDB, Query


class Group(object):

    def __init__(self, id, title, description, tags, snippet, phone, access, isInvitationOnly):
        self.id = id
        self.title = title
        self.description = description
        self.tags = tags
        self.snippet = tags
        self.phone = phone
        self.access = access
        self.isInvitationOnly = isInvitationOnly

    @classmethod
    def from_api_group(cls, group):
        return Group(id=group.id, title=group.title, description=group.description, tags=group.tags, snippet=group.snippet,
                     phone=group.phone, access=group.access, isInvitationOnly=group.isInvitationOnly)


class User(object):

    def __init__(self, id, username, full_name, email, description, thumbnail, role, provider,
                 idp_username, level, user_type, org_id, favorite_group_id, groups):

        self.id = id
        self.username = username
        self.full_name = full_name
        self.email = email
        self.description = description
        self.thumbnail = thumbnail
        self.role = role
        self.provider = provider
        self.idp_username = idp_username
        self.level = level
        self.user_type = user_type
        self.org_id = org_id
        self.favorite_group_id = favorite_group_id
        self.credits = -1
        self.groups = groups

        name_parts = full_name.split(',')
        if len(name_parts) > 2:
            self.last_name = name_parts[0]
            self.first_name = name_parts[-1]
        elif len(name_parts) == 2:
            self.last_name = name_parts[0]
            self.first_name = name_parts[1]

        elif len(name_parts) == 1:
            self.last_name = name_parts[0]
            self.first_name = name_parts[0]
        else:
            raise Exception("Could not parse name from %s" % full_name)

    @classmethod
    def from_api_user(cls, u):

        user = User(u.id, u.username, u.fullName, u.email, u.description, u.thumbnail,
                    u.roleId, u.provider, u.idpUsername, u.level, u.userType, u.orgId,
                    u.favGroupId, groups=[])

        if 'groups' in u:
            for grp in u.groups:
                group = Group.from_api_group(grp)
                user.groups.append(group)

        return user

    def toJson(self):
        return jsonpickle.encode(self)


class MigrationHelper(object):

    def __init__(self, aol_helper, data_folder_name, logger):
        self.aol_helper = aol_helper
        self.logger = logger
        self.data_folder = os.path.join(os.path.dirname(__file__), data_folder_name)
        if not os.path.exists(self.data_folder):
            os.mkdir(self.data_folder)

    def backup(self):
        # backup users
        users = self.aol_helper.gis.users
        users_json_file = os.path.join(self.data_folder, "users.json")
        if os.path.exists(users_json_file):
            os.remove(users_json_file)

        saved_users = []
        for u in users.search():
            user = User.from_api_user(u)
            saved_users.append(user)

    def backup_users(self):
        collection_name = "users.json"
        users = self.aol_helper.gis.users
        db_path = os.path.join(self.data_folder, collection_name)
        if os.path.exists(db_path):
            os.remove(db_path)

        store = TinyDB(db_path)
        saved_users = []
        for u in users.search():
            user = User.from_api_user(u)
            saved_users.append(user)

        user_json = jsonpickle.encode(saved_users)
        store.insert({'users': user_json})
        self.logger.info("saved users to %s" % db_path)

    def backup_groups(self):
        collection_name = "groups.json"
        api_groups = self.aol_helper.gis.groups.search("!owner:esri_* & !Basemaps")
        groups = []
        for ag in api_groups:
            group = Group.from_api_group(ag)
            groups.append(group)

        db_path = os.path.join(self.data_folder, collection_name)
        if os.path.exists(db_path):
            os.remove(db_path)

        store = TinyDB(db_path)
        groups_json = jsonpickle.encode(groups)
        store.insert({'groups': groups_json})
        self.logger.info("saved groups to %s" % db_path)

    def backup_user_content(self, user):
        collection_name = "content.json"
        if user == 'me':
            user = self.aol_helper.gis.users.me

        folders = user.folders
        content_dir = os.path.join(self.data_folder, user.username)

        if not os.path.exists(content_dir):
            os.mkdir(content_dir)

        db_path = os.path.join(content_dir, collection_name)
        if os.path.exists(db_path):
            os.remove(db_path)

        content = {}
        store = TinyDB(db_path)

        content['folders'] = folders
        content['items'] = []
        for item in user.items():
            content['items'].append(item)

        content['user_id'] = user.id
        store.insert({'content': content})

        self.logger.info("Saved content for %s" % user.id)

    def match_users(self, target_migration_helper):

        if not isinstance(target_migration_helper, MigrationHelper):
            raise Exception("target_migration_helper should be instance of MigrationHelper")

        from_db_path = os.path.join(self.data_folder, "users.json")
        from_store = TinyDB(from_db_path)
        from_users = jsonpickle.loads(from_store.all()[0]['users'])

        to_db_path = os.path.join(target_migration_helper.data_folder, "users.json")
        to_store = TinyDB(to_db_path)
        to_users = jsonpickle.loads(to_store.all()[0]['users'])

        matched_users = []
        for from_user in from_users:
            for to_user in to_users:
                if from_user.email == to_user.email:
                    matched_users.append((from_user, to_user))
                    break

        return matched_users

    def _migrate_group(self, source_group, target_portal):

        target_admin_username = 'ujjwal.narayan_bx_content_migration'
        GROUP_COPY_PROPERTIES = ['title', 'description', 'tags', 'snippet', 'phone',
                                 'access', 'isInvitationOnly']

        target_group = {}
        for property_name in GROUP_COPY_PROPERTIES:
            target_group[property_name] = source_group[property_name]

        if source_group.access == 'org' and target_portal.properties['portalMode'] == 'singletenant':
            # cloning from ArcGIS online to ArcGIS Enterprise
            target_group['access'] = 'public'
        elif source_group.access == 'public' and self.aol_helper.gis.properties['portalMode'] == 'singletenant' \
            and target_portal.properties['portalModel'] == 'multitenant' \
            and 'id' in target_portal.properties:
                target_group['access'] = 'org'

        copied_group = target_portal.groups.create_from_dict(target_group)
        members = source_group.get_members()
        if not members['owner'] == target_admin_username:
            copied_group.reassign_to(members['owner'])
        if members['users']:
            copied_group.add_users(members['users'])
        return copied_group

    def migrate_groups(self, target_portal):

        collection_name = "groups.json"
        db_path = os.path.join(self.data_folder, collection_name)
        store = TinyDB(db_path)
        groups_json = store.all()[0]['groups']
        groups = jsonpickle.loads(groups_json)

        for group in groups:
            api_group = self.aol_helper.gis.groups.search("id:%s" % group.id)[0]
            self._migrate_group(api_group, target_portal)

