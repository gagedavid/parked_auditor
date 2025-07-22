import csv
import pandas as pd
import dns.resolver
import socket
from concurrent.futures import ThreadPoolExecutor, as_completed

# ======== CONFIG ========
# File paths and batch settings
INPUT_CSV = "extracted_urls.csv"
RESOLVING_CSV = "resolving_domains_25.csv"
NONRESOLVING_CSV = "nonresolving_domains_25.csv"
PARKED_CSV = "parked_domains_25.csv"
MAX_WORKERS = 500
BATCH_SIZE = 100000
DNS_TIMEOUT = 2.0

# ======== KNOWN PARKED IP ADDRESSES ========
# List of IPs known to be used for parked domains
PARKED_IPS = {
    "34.102.136.180",
    "184.168.131.241",
    "184.168.221.96",
    "184.168.131.233",
    "97.74.104.201",
    "68.178.232.100",
    "50.63.202.40",
    "64.190.62.111",
    "64.190.63.136",
    "64.190.62.22",
    "64.190.63.111",
    "185.53.177.29",
    "185.53.179.6",
    "185.53.177.30",
    "185.53.178.7"
}

# ======== DNS + IP CHECK FUNCTION ========
# Check if a domain resolves via DNS and whether it's hosted on a known parked IP
def check_dns(domain):
    try:
        # Attempt to resolve A record
        answers = dns.resolver.resolve(domain, 'A', lifetime=DNS_TIMEOUT)
        ip_list = [rdata.address for rdata in answers]
        # Check if any IP matches a known parked IP
        is_parked = any(ip in PARKED_IPS for ip in ip_list)
        return {
            "domain": domain,
            "resolves_dns": True,
            "ip_addresses": ",".join(ip_list),
            "parked_ip": is_parked
        }
    except Exception:
        # On failure (timeout, NXDOMAIN, etc.)
        return {
            "domain": domain,
            "resolves_dns": False,
            "ip_addresses": "",
            "parked_ip": False
        }

# ======== PARALLEL BATCH PROCESSING FUNCTION ========
# Process domains in parallel using a thread pool for faster DNS checking
def process_batch(batch):
    results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(check_dns, domain): domain for domain in batch}
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            results.append(result)
            if i % 10000 == 0:
                print(f"🔄 Checked {i}/{len(batch)} in this batch...")
    return results

# ======== MAIN SCRIPT ENTRY POINT ========
def main():
    print("📥 Loading domains from CSV...")
    
    # Read input CSV and clean domain list
    df = pd.read_csv(INPUT_CSV)
    domains = df['domain'].dropna().astype(str).str.strip().tolist()
    print(f"✅ Loaded {len(domains)} domains")

    # Containers for results
    all_resolving = []
    all_nonresolving = []
    all_parked = []

    # Process domains in batches
    for start in range(0, len(domains), BATCH_SIZE):
        end = min(start + BATCH_SIZE, len(domains))
        batch = domains[start:end]
        print(f"\n🚀 Processing batch {start} - {end}...")

        results = process_batch(batch)

        # Categorize results
        for r in results:
            if r["resolves_dns"]:
                all_resolving.append(r)
                if r["parked_ip"]:
                    all_parked.append(r)
            else:
                all_nonresolving.append(r)

        print(f"✅ Batch done: {len(results)} total | {len(all_resolving)} resolving | {len(all_nonresolving)} non-resolving | {len(all_parked)} parked")

    # Save categorized results to separate CSVs
    pd.DataFrame(all_resolving).to_csv(RESOLVING_CSV, index=False)
    pd.DataFrame(all_nonresolving).to_csv(NONRESOLVING_CSV, index=False)
    pd.DataFrame(all_parked).to_csv(PARKED_CSV, index=False)

    print(f"\n💾 Results saved:")
    print(f"   ✅ Resolving domains → {RESOLVING_CSV}")
    print(f"   ❌ Non-resolving domains → {NONRESOLVING_CSV}")
    print(f"   🅿️ Parked domains → {PARKED_CSV}")

# ======== RUN SCRIPT ========
if __name__ == "__main__":
    main()
