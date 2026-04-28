#!/bin/bash

# Acumatica Cloud Function Deployment Script
# This script deploys the Acumatica connector to Firebase Cloud Functions

set -e  # Exit on error

echo "=========================================="
echo "Acumatica Cloud Function Deployment"
echo "=========================================="
echo ""

# Check if Firebase CLI is installed
if ! command -v firebase &> /dev/null; then
    echo "❌ Firebase CLI is not installed."
    echo "Install it with: npm install -g firebase-tools"
    exit 1
fi

echo "✓ Firebase CLI found"

# Check if logged in to Firebase
if ! firebase projects:list &> /dev/null; then
    echo "❌ Not logged in to Firebase."
    echo "Please run: firebase login"
    exit 1
fi

echo "✓ Firebase authentication verified"

# Check if in correct directory
if [ ! -f "firebase.json" ]; then
    echo "❌ firebase.json not found. Please run this script from the acumatica_cloud_function directory."
    exit 1
fi

echo "✓ Configuration files found"
echo ""

# Display current Firebase project
echo "Current Firebase project:"
firebase use

echo ""
read -p "Is this the correct project? (y/n) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Please switch to the correct project with: firebase use <project-id>"
    exit 1
fi

echo ""
echo "Installing Python dependencies..."
cd functions
pip install -r requirements.txt
cd ..

echo ""
echo "Deploying cloud function..."
firebase deploy --only functions:zingworks_acumatica_connector

echo ""
echo "=========================================="
echo "✓ Deployment Complete!"
echo "=========================================="
echo ""
echo "Your function is now available at:"
echo "https://[region]-[project-id].cloudfunctions.net/zingworks_acumatica_connector"
echo ""
echo "Next steps:"
echo "1. Configure credentials in Google Cloud Secret Manager"
echo "2. Test the endpoint with a POST request"
echo "3. Update CORS settings if needed"
echo ""
