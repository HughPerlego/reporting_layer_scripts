/* User group codes */

    SELECT prod.users.id as user_id,
           prod.users.group_id,
           prod.groups.group_name,
           prod.groups.group_code,
           organisation_name
    FROM prod.users
             LEFT JOIN prod.groups On prod.groups.id = prod.users.group_id
             LEFT JOIN prod.organisations ON prod.organisations.id = prod.users.organisation_id
    WHERE prod.users.group_id IS NOT NULL
      and group_id > 0


