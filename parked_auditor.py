import pandas as pd
import requests
import socket
import ssl
import csv
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning, XMLParsedAsHTMLWarning
from concurrent.futures import ThreadPoolExecutor, as_completed
import warnings

# ========== CONFIG ==========
INPUT_CSV = "extracted_urls.csv" #inpurt your csv and the titel you want for output files here
PARKED_OUTPUT = "Parked_domains.csv"
ACTIVE_OUTPUT = "Active_domains.csv"
ERROR_OUTPUT = "Error_domains.csv"
MAX_WORKERS = 500 #change based on system
BATCH_SIZE = 100_000
RESUME_BATCH_INDEX = 0  # Set this to 1 to start from second batch, etc.

# Known parked domain IPs
PARKED_IPS = {
    "34.102.136.180", "184.168.131.241", "184.168.221.96", "184.168.131.233",
    "97.74.104.201", "68.178.232.100", "50.63.202.40", "64.190.62.111",
    "64.190.63.136", "64.190.62.22", "64.190.63.111", "185.53.177.29",
    "185.53.179.6", "185.53.177.30", "185.53.178.7"
}

# Parked keyword indicators
PARKED_KEYWORDS = [
    "buy this domain", "this domain is for sale", "sedo", "bodis", "dan.com",
    "afternic", "parkingcrew", "parked", "advertising", "go daddy", "godaddy"
]

# Suppress HTML parsing warnings
warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# ========== HELPER FUNCTIONS ==========

def get_ip(domain):
    try:
        return socket.gethostbyname(domain)
    except Exception:
        return None

def fetch_html(domain):
    try:
        url = f"http://{domain}"
        response = requests.get(url, timeout=5)
        return response.text
    except Exception:
        return ""

def ssl_cert_title(domain):
    try:
        ctx = ssl.create_default_context()
        with socket.create_connection((domain, 443), timeout=5) as sock:
            with ctx.wrap_socket(sock, server_hostname=domain) as ssock:
                cert = ssock.getpeercert()
                subject = dict(x[0] for x in cert['subject'])
                return subject.get('commonName', '')
    except Exception:
        return ""

def is_parked(domain):
    ip = get_ip(domain)
    if ip in PARKED_IPS:
        return domain, ip, "Matched parked IP"

    html = fetch_html(domain)
    if any(keyword in html.lower() for keyword in PARKED_KEYWORDS):
        return domain, ip, "Matched parked keyword in HTML"

    soup = BeautifulSoup(html, "html.parser")
    title = (soup.title.string or "").lower() if soup.title else ""
    if any(keyword in title for keyword in PARKED_KEYWORDS):
        return domain, ip, "Matched keyword in page title"

    ssl_title = ssl_cert_title(domain).lower()
    if any(keyword in ssl_title for keyword in PARKED_KEYWORDS):
        return domain, ip, "Matched keyword in SSL cert"

    return domain, ip, None

# ========== BATCHED PROCESSING ==========

def process_batch(batch, batch_index):
    parked, active, errors = [], [], []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(is_parked, domain): domain for domain in batch}
        for i, future in enumerate(as_completed(futures), 1):
            try:
                domain, ip, reason = future.result()
                row = {"domain": domain, "ip": ip or "", "reason": reason or "Active"}
                if reason:
                    parked.append(row)
                else:
                    active.append(row)
            except Exception as e:
                errors.append({"domain": futures[future], "error": str(e)})

            if i % 10_000 == 0:
                print(f"🔄 Batch {batch_index + 1}: Processed {i}/{len(batch)} domains")

    return parked, active, errors

# ========== MAIN ==========
def main():
    print("📥 Reading input URLs...")
    df = pd.read_csv(INPUT_CSV)
    domains = df['domain'].dropna().astype(str).str.strip().unique().tolist()
    total_batches = (len(domains) + BATCH_SIZE - 1) // BATCH_SIZE

    for batch_index in range(RESUME_BATCH_INDEX, total_batches):
        start = batch_index * BATCH_SIZE
        end = min((batch_index + 1) * BATCH_SIZE, len(domains))
        batch = domains[start:end]

        print(f"\n🚀 Processing batch {batch_index + 1}/{total_batches}: {start}–{end}")
        parked, active, errors = process_batch(batch, batch_index)

        mode = 'a' if batch_index > 0 else 'w'
        pd.DataFrame(parked).to_csv(PARKED_OUTPUT, mode=mode, index=False, header=(mode == 'w'))
        pd.DataFrame(active).to_csv(ACTIVE_OUTPUT, mode=mode, index=False, header=(mode == 'w'))
        pd.DataFrame(errors).to_csv(ERROR_OUTPUT, mode=mode, index=False, header=(mode == 'w'))

        print(f"✅ Saved {len(parked)} parked | {len(active)} active | {len(errors)} errors")

if __name__ == "__main__":
    main()

