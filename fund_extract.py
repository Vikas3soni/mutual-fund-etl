import requests
from utils import upload_blob_gcs

bucket_name = "my-test-bucket-mf-101"

def extract_data(url):
    """
    function to extract data from source server
    """
    #target_url = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx?mf=53&frmdt=01-Jun-2021&todt=18-Apr-2022"
    target_url = url
    f = requests.get(target_url)
    f.encoding = 'utf-8'
    file_name = 'mf_data_{0}'.format(timezone.utcnow())
    gcs_file_name = file_name
    file = open(file_name, 'wb')
    for line in f:
      file.write(line)
    # write data to gcs bucket
    upload_blob_gcs(bucket_name, file_name, gcs_file_name)
