def metaCloud():
    from google.cloud import storage
    import os
    from google.auth.transport import requests
    import google.auth.transport.requests

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/tier1marketspace/youtuberReport/fourth-splice-313717-968799892cd2.json"

    storage_client = storage.Client()

    buckets = list(storage_client.list_buckets())

    bucket = storage_client.get_bucket("tom-market-report")
    blob = bucket.blob('meta/winners_final.csv')
    blob2 = bucket.blob('meta/newsletter.csv')

    blob.upload_from_filename('/home/tier1marketspace/youtuberReport/scripts/data/winners_final.csv')
    blob2.upload_from_filename('/home/tier1marketspace/youtuberReport/scripts/data/newsletter.csv')
    print(buckets)

metaCloud()