import csv

RTF_FILE = "701 category.rtf"           # Your .rtf (actually plain text)
OUTPUT_FILE = "extracted_urls.csv"    # Clean output

def extract_domains_plaintext(input_path, output_path):
    with open(input_path, "r", encoding="utf-8") as file:
        lines = [line.strip() for line in file.readlines() if line.strip()]

    print(f"📥 Read {len(lines)} lines (treating .rtf as plain text)")

    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["domain"])  # Header
        for line in lines:
            domain = line.split(",")[0].strip()
            if domain:
                writer.writerow([domain])

    print(f"✅ Extracted {len(lines)} domains to {output_path}")

if __name__ == "__main__":
    extract_domains_plaintext(RTF_FILE, OUTPUT_FILE)
