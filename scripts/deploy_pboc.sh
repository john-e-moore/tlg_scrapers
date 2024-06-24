#!/bin/bash

# Config variables
FUNCTION_NAME="pbocBronze"
ZIP_FILE="pbocBronze.zip"
PROJECT_DIR="$(pwd)"
SOURCE_DIR="${PROJECT_DIR}/src/"
TESTS_DIR="${PROJECT_DIR}/src/tests/"
VENV_DIR="${PROJECT_DIR}/venv_lambda/"
# S3_BUCKET and S3_KEY_PBOC should be environment variables

# Run unit tests
echo "Running unit tests..."
python3 -m unittest discover -s $TESTS_DIR -p 'test_*.py'

# Check if tests passed
if [ $? -eq 0 ]; then
    echo "Unit tests passed. Proceeding with deployment..."
else
    echo "Unit tests failed. Aborting deployment."
    exit 1
fi

# Delete old zip file if it exists
if [ -f "$PROJECT_DIR/$ZIP_FILE" ]; then
    echo "Deleting old zip file..."
    rm "$PROJECT_DIR/$ZIP_FILE"
fi

# Package dependencies
echo "Packaging function and dependencies..."
cd $VENV_DIR/lib/python3.*/site-packages/
zip -r9 $PROJECT_DIR/$ZIP_FILE .
# Package source code
cd $SOURCE_DIR
#echo "Current dir: $(pwd)"
#exit 1
zip -r $PROJECT_DIR/$ZIP_FILE *

# Print the size of the newly made zip file in MB
cd $PROJECT_DIR
ZIP_SIZE_MB=$(du -m "$ZIP_FILE" | cut -f1)
echo "The size of the zip file is $ZIP_SIZE_MB MB."

# Upload the zip file to S3
echo "Uploading $ZIP_FILE to S3..."
aws s3 cp "$ZIP_FILE" "s3://$S3_BUCKET/$S3_KEY_PBOC/$ZIP_FILE"

# Deploy the update to AWS Lambda using S3
echo "Updating AWS Lambda function to use the uploaded S3 object..."
echo "Bucket: $S3_BUCKET"
echo "Key: $S3_KEY_PBOC/$ZIP_FILE"
aws lambda update-function-code --function-name $FUNCTION_NAME --s3-bucket $S3_BUCKET --s3-key "$S3_KEY_PBOC/$ZIP_FILE"

echo "Deployment completed."
