from django import forms
from django.contrib.auth.models import User
from .models import Profile

class ProfileForm(forms.ModelForm):
    full_name = forms.CharField(max_length=100, required=True)
    phone = forms.CharField(max_length=15, required=False)

    class Meta:
        model = User
        fields = ['email']

class EditProfileForm(forms.ModelForm):
    full_name = forms.CharField(max_length=150, required=False, label='Full Name')
    email = forms.EmailField(required=True, label='Email')

    class Meta:
        model = User
        fields = ['full_name', 'email']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['full_name'].initial = self.instance.get_full_name()
        self.fields['email'].initial = self.instance.email

    def save(self, commit=True):
        user = super().save(commit=False)
        full_name = self.cleaned_data.get('full_name', '')
        if full_name:
            names = full_name.split(' ', 1)
            user.first_name = names[0]
            user.last_name = names[1] if len(names) > 1 else ''
        user.email = self.cleaned_data['email']
        if commit:
            user.save()
        return user

class ProfileEditForm(forms.ModelForm):
    full_name = forms.CharField(max_length=150, required=False, label='Full Name')
    email = forms.EmailField(required=True, label='Email')
    role = forms.ChoiceField(choices=Profile.ROLE_CHOICES, required=True, label='Role')

    class Meta:
        model = Profile
        fields = ['role']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.instance and self.instance.user:
            self.fields['full_name'].initial = self.instance.user.get_full_name()
            self.fields['email'].initial = self.instance.user.email

    def save(self, commit=True):
        profile = super().save(commit=False)
        
        # Update user information
        user = profile.user
        full_name = self.cleaned_data.get('full_name', '')
        if full_name:
            names = full_name.split(' ', 1)
            user.first_name = names[0]
            user.last_name = names[1] if len(names) > 1 else ''
        user.email = self.cleaned_data['email']
        
        if commit:
            user.save()
            profile.save()
        return profile
