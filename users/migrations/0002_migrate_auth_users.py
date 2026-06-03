"""
Data migration: copy rows from auth_user -> users_user.

The only users in the DB are auto-created service accounts from
InternalTokenBackend (e.g., 'nexus'). They will be re-created on first
auth after migration, so this migration is best-effort — if auth_user
does not exist yet (fresh DB) or is empty, it gracefully does nothing.

We also truncate django_admin_log to avoid FK constraint errors
pointing at the old auth_user table.
"""

from django.db import migrations


def forwards(apps, schema_editor):
    connection = schema_editor.connection
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'auth_user' AND table_schema = 'public'"
        )
        if not cursor.fetchone():
            return

        cursor.execute("SELECT COUNT(*) FROM auth_user")
        if cursor.fetchone()[0] == 0:
            return

        cursor.execute(
            """
            INSERT INTO users_user (id, password, last_login, is_superuser, email, username, is_active, is_staff)
            SELECT id, password, last_login, is_superuser,
                   COALESCE(NULLIF(email, ''), username || '@internal.service'),
                   username, is_active, is_staff
            FROM auth_user
            ON CONFLICT (id) DO NOTHING
            """
        )

        cursor.execute(
            "SELECT setval(pg_get_serial_sequence('users_user', 'id'), "
            "COALESCE((SELECT MAX(id) FROM users_user), 1))"
        )

        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'auth_user_groups' AND table_schema = 'public'"
        )
        if cursor.fetchone():
            cursor.execute(
                """
                INSERT INTO users_user_groups (user_id, group_id)
                SELECT user_id, group_id FROM auth_user_groups
                ON CONFLICT DO NOTHING
                """
            )

        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'auth_user_user_permissions' AND table_schema = 'public'"
        )
        if cursor.fetchone():
            cursor.execute(
                """
                INSERT INTO users_user_user_permissions (user_id, permission_id)
                SELECT user_id, permission_id FROM auth_user_user_permissions
                ON CONFLICT DO NOTHING
                """
            )

        cursor.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_name = 'django_admin_log' AND table_schema = 'public'"
        )
        if cursor.fetchone():
            cursor.execute("TRUNCATE django_admin_log")


class Migration(migrations.Migration):
    dependencies = [
        ("users", "0001_initial"),
    ]

    operations = [
        migrations.RunPython(forwards, migrations.RunPython.noop),
    ]
