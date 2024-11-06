from google.cloud import storage
import pandas as pd
import io

# Initialize the GCS client
client = storage.Client()
bucket = client.get_bucket('your-bucket-name')
blob = bucket.blob('path/to/your-file.csv')

# Read the single-column CSV directly from GCS into a DataFrame
file_stream = io.BytesIO(blob.download_as_bytes())
df = pd.read_csv(file_stream)

# Modify the values in the single column
df[df.columns[0]] = df[df.columns[0]].replace({'old_value': 'new_value'})  # Adjust 'old_value' and 'new_value'

# Convert the modified DataFrame back to CSV format in memory
output_stream = io.StringIO()
df.to_csv(output_stream, index=False)
output_stream.seek(0)

# Upload the modified CSV back to GCS, overwriting the original file
blob.upload_from_string(output_stream.getvalue(), content_type='text/csv')
