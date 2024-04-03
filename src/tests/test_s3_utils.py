import unittest
from unittest.mock import patch
from moto import mock_aws
import boto3
import pandas as pd
from common.s3_utils import S3Utility 
from common.config_utils import load_config
from io import StringIO

class TestS3Utility(unittest.TestCase):
    
    @mock_aws
    def setUp(self):
        # Start mock AWS instance
        self.mock_aws = mock_aws()
        self.mock_aws.start()

        # Set up test resources
        self.bucket = 'test-bucket'
        self.prefix = 'test'
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        self.s3_client.create_bucket(Bucket=self.bucket)
        self.test_key = f'{self.prefix}/file.csv'
        self.local_file_path = 'test_file.csv'
        self.df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})

    @mock_aws
    def tearDown(self):
        self.mock_aws.stop()

    @mock_aws
    def test_upload_local_file_to_s3(self):
        with open(self.local_file_path, 'w') as f:
            f.write('Some test data')
        
        S3Utility.upload_local_file_to_s3(self.local_file_path, self.bucket, self.test_key)
        
        response = self.s3_client.get_object(Bucket=self.bucket, Key=self.test_key)
        content = response['Body'].read().decode('utf-8')
        self.assertEqual(content, 'Some test data')

    @mock_aws
    def test_download_s3_file_to_local(self):
        self.s3_client.put_object(Bucket=self.bucket, Key=self.test_key, Body='Test content')
        S3Utility.download_s3_file_to_local(self.local_file_path, self.bucket, self.test_key)

        with open(self.local_file_path, 'r') as f:
            content = f.read()
        self.assertEqual(content, 'Test content')

    @mock_aws
    def test_download_s3_to_dataframe(self):
        content = self.df.to_csv(index=False)
        self.s3_client.put_object(Bucket=self.bucket, Key=f'{self.prefix}/file.csv', Body=content)
        df = S3Utility.download_s3_to_dataframe(self.bucket, f'{self.prefix}/file.csv')
        pd.testing.assert_frame_equal(df, self.df)

    @mock_aws
    def test_upload_dataframe_s3(self):
        key = f'{self.prefix}/file.csv'
        S3Utility.upload_dataframe_s3(self.df, self.bucket, key, 'csv')

        response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        df_from_s3 = pd.read_csv(StringIO(content))
        pd.testing.assert_frame_equal(df_from_s3, self.df)

if __name__ == '__main__':
    unittest.main()
