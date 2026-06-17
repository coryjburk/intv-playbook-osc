#!/usr/bin/env python3
import json
import urllib.request
import xml.etree.ElementTree as ET
import sys
from datetime import datetime

# Industry target reference platforms mapping dictionary
FEEDS_CONFIG = {
    "McKinsey Operations": "https://www.mckinsey.com/insights/rss/operations",
    "Supply Chain Dive": "https://www.supplychaindive.com/feeds/news/",
    "Harvard Business Review": "https://hbr.org/rss/topic/operations"
}

def clean_tag(text):
    if not text: return ""
    return text.strip().replace("\n", " ")

def run_pipeline():
    print(f"[{datetime.now().isoformat()}] Launching Industry Intelligence Aggregator Pipeline...")
    output_bundle = {"articles": [], "compiled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
    
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AssetEngine/1.0"}
    
    for source_name, feed_url in FEEDS_CONFIG.items():
        print(f"Connecting to stream: {source_name}...")
        try:
            req = urllib.request.Request(feed_url, headers=headers)
            with urllib.request.urlopen(req, timeout=15) as response:
                xml_data = response.read()
                
            root = ET.fromstring(xml_data)
            # Support both standard RSS 2.0 channel item nodes architectures cleanly
            channel = root.find("channel")
            items = channel.findall("item") if channel is not None else root.findall(".//item")
            
            count = 0
            for item in items:
                if count >= 4: break # Extract top 4 highly relevant items per source site
                title = clean_tag(item.find("title").text if item.find("title") is not None else "Operational Update")
                link = clean_tag(item.find("link").text if item.find("link") is not None else "https://www.ascm.org")
                desc = clean_tag(item.find("description").text if item.find("description") is not None else "Access technical research portals for details.")
                pub_date = clean_tag(item.find("pubDate").text[:16] if item.find("pubDate") is not None else "Current Analysis")
                
                # Strip HTML tags out of snippets for clean JSON formatting
                if "<" in desc:
                    desc = desc.split("<")[0] if desc.split("<")[0] else "Access core intelligence briefings directly."
                if len(desc) > 180:
                    desc = desc[:177] + "..."
                    
                output_bundle["articles"].append({
                    "source": source_name,
                    "title": title,
                    "link": link,
                    "snippet": desc,
                    "date": pub_date
                })
                count += 1
            print(f"Extracted {count} records cleanly from {source_name}.")
        except Exception as e:
            print(f"Warning: Stream pipeline connection error on {source_name}: {str(e)}", file=sys.stderr)
            
    # Write output to the workspace directory file
    try:
        with open("feeds.json", "w", encoding="utf-8") as f:
            json.dump(output_bundle, f, indent=2, ensure_ascii=False)
        print("Data extraction successful. 'feeds.json' written completely.")
    except Exception as e:
        print(f"Critical File System write crash: {str(e)}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    run_pipeline()
