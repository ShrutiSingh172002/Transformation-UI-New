from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth import authenticate, login, logout
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.http import JsonResponse, FileResponse, Http404
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST
from django.contrib.auth.models import User
from django.contrib.auth.forms import PasswordChangeForm
from django.contrib.auth import update_session_auth_hash
from django.core.mail import send_mail
from django.conf import settings
from rest_framework.views import APIView
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response
from rest_framework import status
from .models import Profile
from .forms import (
    ProfileForm, EditProfileForm, ProfileEditForm
)
import os
import json
from datetime import datetime


def send_welcome_email(user_email):
    subject = "Welcome to Our Service"
    message = "Thank you for registering with us!"
    email_from = settings.EMAIL_HOST_USER
    recipient_list = [user_email]
    send_mail(subject, message, email_from, recipient_list)

taskcompleted = False
runcnt = 1

# Create your views here.

class RegisterView(APIView):
    def post(self, request, *args, **kwargs):
        serializer = RegisterSerializer(data=request.data)
        
        if serializer.is_valid():
            user = serializer.save()  # Create the user using the custom `create` method in the serializer
            return Response({'detail': 'User registered successfully.'}, status=status.HTTP_201_CREATED)
        
        return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

class ProtectedView(APIView):
    permission_classes = [IsAuthenticated]

    def get(self, request):
        return Response({"message": f"Hello {request.user.username}, you're authenticated!"})

def index_page(request):
    return render(request, "index.html")
def register(request):
    return render(request, 'register.html')
   
def contact_page(request):
    if request.method == 'POST':
        name = request.POST.get('name')
        email = request.POST.get('email')
        subject = request.POST.get('subject')
        message = request.POST.get('message')

        # You can store it in DB or send an email — example below sends email
        try:
            full_message = f"Message from {name} <{email}>:\n\n{message}"
            send_mail(subject, full_message, settings.DEFAULT_FROM_EMAIL, ['contact@cloudnest.com'])
            return render(request, 'contact.html', {'success': True})
        except Exception as e:
            return render(request, 'contact.html', {'error': str(e)})

    return render(request, "contact.html")

   
def services_page(request):
    return render(request, "services.html")
   
def about_page(request):
    return render(request, "about.html")

def privacy_policy(request):
    return render(request, 'privacy_policy.html')

def terms_conditions(request):
    return render(request, 'terms_conditions.html')

def services(request):
    return render(request, 'services.html')



def upload_template(request):
    # Temporarily disabled for login testing
    return JsonResponse({"message": "Upload functionality temporarily disabled for testing"}, status=503)
        
def download_file(request,filename):
    file_path = os.path.join('C:/DataVapte/OutPut', filename)
    if os.path.exists(file_path):
        return FileResponse(open(file_path, 'rb'), as_attachment=True)
    else:
        raise Http404("File not found")

def download_view(request, filename):
    file_path = os.path.join('C:/DataVapte/OutPut', filename)
    
    # If the file exists, show the download button, else raise 404 error
    if os.path.exists(file_path):
        return render(request, 'download.html', {'filename': filename})
    else:
        raise Http404("File not found")

def transformation_login(request):
    if request.method == 'POST':
        username = request.POST['username']
        password = request.POST['password']
        license_id = request.POST['license_id']

        user = authenticate(request, username=username, password=password)
        if user is not None and user.profile.license_id == license_id:
            login(request, user)
            return redirect('transformation_dashboard')  # Protected transformation view
        else:
            error = "Invalid credentials or license ID"
            return render(request, 'transformation_login.html', {'error': error})
    return render(request, 'transformation_login.html')

@login_required
def user_profile(request):
    user = request.user

    if request.method == 'POST':
        form = ProfileForm(request.POST)
        if form.is_valid():
            user.email = form.cleaned_data['email']
            name_parts = form.cleaned_data['full_name'].split()
            user.first_name = name_parts[0]
            user.last_name = " ".join(name_parts[1:]) if len(name_parts) > 1 else ''
            user.save()
            return redirect('user_profile')
    else:
        full_name = f"{user.first_name} {user.last_name}".strip()
        form = ProfileForm(initial={
            'full_name': full_name,
            'email': user.email,
            'phone': '',  # Update here if you store phone separately
        })

    return render(request, 'profile.html', {'form': form})

@login_required
def edit_profile(request):
    user = request.user
    if request.method == 'POST':
        form = ProfileEditForm(request.POST, instance=user.profile)
        if form.is_valid():
            form.save()
            messages.success(request, 'Profile updated successfully!')
            return redirect('profile')
    else:
        form = ProfileEditForm(instance=user.profile)
    return render(request, 'edit_profile.html', {'form': form})

@login_required
def user_list(request):
    """View to list all users with their roles (admin only)"""
    if not request.user.profile.role == 'admin':
        messages.error(request, 'Access denied. Admin role required.')
        return redirect('profile')
    
    users = User.objects.all().select_related('profile')
    return render(request, 'user_list.html', {'users': users})

@login_required
def projects_view(request):
    return render(request, 'settings.html')

@login_required
def settings_view(request):
    return render(request, 'settings_page.html')

@login_required
def dashboard_view(request):
    """Dashboard view for users"""
    return render(request, 'dashboard.html')


