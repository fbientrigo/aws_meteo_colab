#!/bin/bash

# Check if conda environment 'pangu' is active
if [[ "$CONDA_DEFAULT_ENV" != "pangu" ]]; then
    echo "⚠️  WARNING: Conda environment 'pangu' is NOT active."
    echo "Current environment: $CONDA_DEFAULT_ENV"
    echo "Please activate it with: conda activate pangu"
    # We don't exit here to allow running in environments where CONDA_DEFAULT_ENV might not be set but python is correct
else
    echo "✅ Conda environment 'pangu' is active."
fi

echo "Installing dependencies..."
pip install -r frontend/requirements_frontend.txt

echo "Running Smoke Test..."
python frontend/tests/test_smoke.py

if [ $? -eq 0 ]; then
    echo "✅ Setup Complete. To run the app:"
    echo "streamlit run frontend/app.py"
else
    echo "❌ Smoke Test Failed."
    exit 1
fi
