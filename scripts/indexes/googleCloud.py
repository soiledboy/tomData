def indexCloud():
    from google.cloud import storage
    import os
    from google.auth.transport import requests
    import google.auth.transport.requests

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/tier1marketspace/youtuberReport/fourth-splice-313717-968799892cd2.json"

    storage_client = storage.Client()

    buckets = list(storage_client.list_buckets())

    bucket = storage_client.get_bucket("tom-market-report")

    blob = bucket.blob('indexes/setData.csv')

    blob.upload_from_filename('/home/tier1marketspace/youtuberReport/scripts/data/setData.csv')

    blob = bucket.blob('indexes/setDataCards.csv')

    blob.upload_from_filename('/home/tier1marketspace/youtuberReport/scripts/data/setDataCards_final.csv')

    print(buckets)



