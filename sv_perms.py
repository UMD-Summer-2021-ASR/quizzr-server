# Include several utility methods for permission management.
# May be used later.
class QuizzrPerms:
    permissions = [
        ("normal", [
            "create_own_profile",
            "modify_own_profile",
            "delete_own_profile"
        ]),
        ("admin", [
            "modify_other_profiles",
            "delete_other_profiles"
        ])
    ]

    def __init__(self, users):
        self.users = users

    def get_permissions(self, perm_level: str):
        found_perm = False
        p_list = []
        for p in self.permissions:
            p_list += p[1]
            if p[0] == perm_level:
                found_perm = True
                break
        if not found_perm:
            raise ValueError(f"Unknown permission '{perm_level}'")
        return p_list

    def has_permission(self, user_id, action):
        profile = self.users.find_one({"_id": user_id})
        p_list = self.get_permissions(profile["permLevel"])
        return action in p_list
