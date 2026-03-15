🔐 Authentication Logic (OAuth / Token‑based)

* Secure authentication (no hard‑coded secrets)
* Credentials managed via environment variables
* Enterprise‑ready API access pattern

```python
def get_access_token():
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": os.getenv("LWA_REFRESH_TOKEN"),
        "client_id": os.getenv("LWA_CLIENT_ID"),
        "client_secret": os.getenv("LWA_CLIENT_SECRET")
    }
    response = requests.post(token_url, data=payload)
    access_token = response.json()["access_token"]
    return access_token
```

📅 Dynamic Date Logic (No Manual Inputs)

* Fully automated time window calculation
* No manual parameter changes week to week
* Aligned to business reporting periods

```python
def get_previous_retail_week():
    today = datetime.now(timezone.utc)
    last_saturday = today - timedelta(days=(today.weekday() - 5) % 7)
    second_last_sunday = last_saturday - timedelta(days=6)
    return start_date, end_date
    ```
