import requests

APIKEY = "917881f0-62a2-4f37-a826-bf08ef581239"
OWNER_REF_KEY = "01ef0a7e-0274-11f0-93bf-2ae983d8a0f0"
PROD = "https://app.enote.vet/7edc4405-f8d6-4022-9999-8186ee1ce262"
COPY = PROD + "-copy"

def probe(base, param):
    url = f"{base}/hs/api/v2/GetClient"
    r = requests.get(url, headers={"apikey": APIKEY, "Accept":"application/json"},
                     params={param: OWNER_REF_KEY}, timeout=15)
    print(f"\n=== {base}  ?{param}=... ===")
    print(f"[{r.status_code}] {r.url}")
    print("Headers:", dict(r.headers))
    print("Body preview:", (r.text or "")[:300])
    try:
        print("JSON:", r.json())
    except Exception:
        print("JSON: <parse error>")

if __name__ == "__main__":
    probe(PROD, "id")
    probe(PROD, "Ref_Key")
    probe(COPY, "id")
    probe(COPY, "Ref_Key")
