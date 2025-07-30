from django.core.management.base import BaseCommand
from django.contrib.auth.models import User
from apptransformation.models import Profile

class Command(BaseCommand):
    help = 'Assign roles to users'

    def add_arguments(self, parser):
        parser.add_argument(
            '--admin-users',
            nargs='+',
            type=str,
            help='List of usernames to assign admin role'
        )
        parser.add_argument(
            '--member-users',
            nargs='+',
            type=str,
            help='List of usernames to assign member role'
        )
        parser.add_argument(
            '--default-admin',
            action='store_true',
            help='Assign admin role to the first user (superuser)'
        )

    def handle(self, *args, **options):
        # Assign admin role to specified users
        if options['admin_users']:
            for username in options['admin_users']:
                try:
                    user = User.objects.get(username=username)
                    profile, created = Profile.objects.get_or_create(user=user)
                    profile.role = 'admin'
                    profile.save()
                    self.stdout.write(
                        self.style.SUCCESS(f'Successfully assigned admin role to {username}')
                    )
                except User.DoesNotExist:
                    self.stdout.write(
                        self.style.ERROR(f'User {username} does not exist')
                    )

        # Assign member role to specified users
        if options['member_users']:
            for username in options['member_users']:
                try:
                    user = User.objects.get(username=username)
                    profile, created = Profile.objects.get_or_create(user=user)
                    profile.role = 'member'
                    profile.save()
                    self.stdout.write(
                        self.style.SUCCESS(f'Successfully assigned member role to {username}')
                    )
                except User.DoesNotExist:
                    self.stdout.write(
                        self.style.ERROR(f'User {username} does not exist')
                    )

        # Assign admin role to the first superuser
        if options['default_admin']:
            try:
                superuser = User.objects.filter(is_superuser=True).first()
                if superuser:
                    profile, created = Profile.objects.get_or_create(user=superuser)
                    profile.role = 'admin'
                    profile.save()
                    self.stdout.write(
                        self.style.SUCCESS(f'Successfully assigned admin role to superuser {superuser.username}')
                    )
                else:
                    self.stdout.write(
                        self.style.WARNING('No superuser found')
                    )
            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f'Error assigning admin role: {e}')
                )

        # Show current role assignments
        self.stdout.write('\nCurrent role assignments:')
        for profile in Profile.objects.all():
            self.stdout.write(f'{profile.user.username}: {profile.role}') 