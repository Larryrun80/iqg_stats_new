from flask_principal import Permission, RoleNeed

anonymous_permission = Permission()
admin_permission = Permission(RoleNeed('admin'))
marketing_permission = Permission(RoleNeed('marketing'))
