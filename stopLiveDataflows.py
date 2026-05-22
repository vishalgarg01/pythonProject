import requests
import time

# List of oldIds (paste yours here)
old_ids = [
"f44e1bb3-a2b4-3245-8f5e-1809afecb1e4","e31669b7-019c-1000-0000-00005fe51612","2eb514a1-e845-373c-865a-217c30e46346","7a29987c-c5b6-37b4-b4a2-4f52a38d5dff","2eb514a1-e845-373c-96f3-aaea13760e54","2eb514a1-e845-373c-b8b2-0cc3f6b5309b","7a29987c-c5b6-37b4-97b5-9fc33d73b0d6","7a29987c-c5b6-37b4-968e-c95e87aea0ea","2eb514a1-e845-373c-abd1-9020cd0f1c62","2eb514a1-e845-373c-89b3-7f53906531e3","7a29987c-c5b6-37b4-b40f-cf64ae4863b7","c9a47d35-2aed-3818-bf0a-f65f0fc9fcdf","c9a47d35-2aed-3818-bfc8-81d15c75f0f1","2eb514a1-e845-373c-9790-c32e11a81ffd","2eb514a1-e845-373c-959c-3ec4e7288865","c8454c23-ffd4-322e-aea4-8b265c4c0c3e","c9a47d35-2aed-3818-a107-7fc5152193c3"
]

BASE_URL = "https://eucrm.connectplus.capillarytech.com/api/workspaces/198/dataflows/{}/state/Live"

headers = {
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Content-Length": "0",
    "Origin": "https://eucrm.connectplus.capillarytech.com",
    "Referer": "https://eucrm.connectplus.capillarytech.com/connect/ui",
    "User-Agent": "Mozilla/5.0",
    "content-type": "application/json",
}

cookies = {
    "SESSION": "YmI5NGVhYzgtMTU5YS00ODc3LTlkZmQtNjI1ZTg0M2NiNjcz"
}

for dataflow_id in old_ids:
    url = BASE_URL.format(dataflow_id)
    params = {
        "time": int(time.time() * 1000)
    }

    try:
        response = requests.put(url, headers=headers, cookies=cookies, params=params)

        if response.status_code == 200:
            print(f"✅ Success: {dataflow_id}")
        else:
            print(f"❌ Failed: {dataflow_id} | Status: {response.status_code} | Response: {response.text}")

    except Exception as e:
        print(f"⚠️ Error for {dataflow_id}: {e}")

    # Optional: small delay to avoid rate limits
    time.sleep(0.2)