import requests
import json
import csv
import time
import random
import os
from collections import defaultdict

# Use the same headers from your test.py
HEADERS = {
    'accept': 'application/json',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'cookie': 'PHPSESSID=68c8bcd41607c285dead0ec1fd66ca97; osbid=3; osbk=77eae7a9e5dfe7c670e76ce054f78c6740245f15', 
    'origin': 'https://merchant.matchingnumber.com',
    'referer': 'https://merchant.matchingnumber.com/eng/gestione/index?class=Dati_Prodotto',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest',
}

TOTAL_PAGES = 148  # Your original setting
PAGE_SIZE = 20
OUTPUT_CSV_FILE = 'products_with_real_compatibility.csv'
IMAGE_DIRECTORY = 'images'

def download_image(url, folder_path, session):
    """Downloads a single image and saves it to a specified folder."""
    if not url:
        return
    try:
        image_filename = url.split('/')[-1].split('?')[0]
        if not image_filename:
            return

        os.makedirs(folder_path, exist_ok=True)
        filename_path = os.path.join(folder_path, image_filename)
        
        if os.path.exists(filename_path):
            return

        with session.get(url, stream=True, timeout=15) as response:
            response.raise_for_status()
            if 'image' in response.headers.get('content-type', '').lower():
                with open(filename_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
    except requests.exceptions.RequestException as e:
        print(f"    - Error downloading {url}. {e}")

def fetch_all_data(session, base_url, class_name, description):
    """Generic function to fetch all data from an API endpoint."""
    print(f"\n📋 Fetching {description}...")
    
    all_records = []
    page_num = 1
    
    while True:
        try:
            params = {'rand': [random.randint(1000, 9999), random.randint(1000, 9999)]}
            data = {'pageNo': page_num, 'pageSize': 100, 'sort': '', 'dir': '', 'session': 'false', 'sessionId': ''}
            
            response = session.post(f"{base_url}/class/{class_name}", params=params, data=data, timeout=20)
            response.raise_for_status()
            
            result = response.json()
            records = result.get('records', [])
            
            if not records:
                break
                
            all_records.extend(records)
            print(f"   Page {page_num}: {len(records)} records (Total: {len(all_records)})")
            
            # Check if we got all records
            total_count = int(result.get('totalCount', 0))
            if len(all_records) >= total_count:
                break
                
            page_num += 1
            time.sleep(0.5)  # Be polite to the server
            
        except Exception as e:
            print(f"   ❌ Error on page {page_num}: {e}")
            break
    
    print(f"✅ Fetched {len(all_records)} {description}")
    return all_records

def build_compatibility_lookup(compatibility_records, relationship_records):
    """Build a lookup table from product ID to compatibility information."""
    print(f"\n🔗 Building compatibility lookup...")
    
    # Create compatibility lookup by ID
    compatibility_by_id = {}
    for comp in compatibility_records:
        compatibility_by_id[comp['id']] = comp
    
    # Create product-to-compatibility mapping
    product_compatibility = defaultdict(list)
    
    for rel in relationship_records:
        product_id = rel.get('id_prodotti')
        compatibility_id = rel.get('id_compatibilita')
        
        if product_id and compatibility_id and compatibility_id in compatibility_by_id:
            comp_data = compatibility_by_id[compatibility_id]
            compatibility_info = {
                'brand': comp_data.get('marchi', ''),
                'model': comp_data.get('modelli', ''),
                'version': comp_data.get('allestimenti', ''),
                'year_from': comp_data.get('da_anno_modello', ''),
                'year_to': comp_data.get('a_anno_modello', ''),
                'displacement': comp_data.get('cilindrata', '')
            }
            product_compatibility[product_id].append(compatibility_info)
    
    print(f"✅ Built compatibility lookup for {len(product_compatibility)} products")
    return product_compatibility

def format_compatibility_string(compatibility_list):
    """Format compatibility list into a readable string."""
    if not compatibility_list:
        return ""
    
    formatted_parts = []
    for comp in compatibility_list:
        parts = []
        
        if comp['brand']:
            parts.append(comp['brand'])
        if comp['model']:
            parts.append(comp['model'])
        if comp['version'] and comp['version'] != comp['model']:
            parts.append(comp['version'])
        
        # Add year range if available
        if comp['year_from'] and comp['year_to']:
            if comp['year_from'] == comp['year_to']:
                parts.append(f"({comp['year_from']})")
            else:
                parts.append(f"({comp['year_from']}-{comp['year_to']})")
        elif comp['year_from']:
            parts.append(f"(from {comp['year_from']})")
        elif comp['year_to']:
            parts.append(f"(until {comp['year_to']})")
        
        # Add displacement if available and significant
        if comp['displacement'] and comp['displacement'] != '0':
            parts.append(f"{comp['displacement']}cc")
        
        if parts:
            formatted_parts.append(" ".join(parts))
    
    return "; ".join(formatted_parts)

def scrape_products_with_real_compatibility():
    """
    Main function to scrape products with real compatibility data from the relationship table.
    """
    base_url = 'https://merchant.matchingnumber.com/eng/gestione/getlist'
    gallery_image_base_url = 'https://merchant.matchingnumber.com/scripts/imgresize.php?pic=immagini/articoli/'
    
    print(f"🚀 STARTING COMPLETE COMPATIBILITY EXTRACTION")
    print("=" * 60)
    print(f"This will fetch:")
    print(f"  1. All compatibility records (~3,240)")
    print(f"  2. All product-compatibility relationships") 
    print(f"  3. All products with their real compatibility data")
    print("=" * 60)
    
    os.makedirs(IMAGE_DIRECTORY, exist_ok=True)
    
    with requests.Session() as session:
        session.headers.update(HEADERS)
        
        # 1. Fetch all compatibility records
        compatibility_records = fetch_all_data(
            session, base_url, 'Dati_Compatibilita', 'compatibility records'
        )
        
        # 2. Fetch all product-compatibility relationships
        relationship_records = fetch_all_data(
            session, base_url, 'Dati_Prodotto_Compatibilita', 'product-compatibility relationships'
        )
        
        # 3. Build compatibility lookup
        product_compatibility = build_compatibility_lookup(compatibility_records, relationship_records)
        
        # 4. Fetch products with compatibility
        print(f"\n📋 Fetching products with compatibility data...")
        all_products_data = []
        
        for page_num in range(1, TOTAL_PAGES + 1):
            print(f"\n--- Processing Products Page {page_num}/{TOTAL_PAGES} ---")
            
            params = {'rand': [random.randint(1000, 9999), random.randint(1000, 9999)]}
            data = {'pageNo': page_num, 'pageSize': PAGE_SIZE, 'sort': '', 'dir': '', 'session': 'false', 'sessionId': ''}
            
            try:
                response = session.post(f"{base_url}/class/Dati_Prodotto", params=params, data=data, timeout=20)
                response.raise_for_status()
                records = response.json().get('records', [])
                
                if not records:
                    print("No more records found. Stopping.")
                    break
                
                for record in records:
                    product_id = record.get('id')
                    product_name = record.get('nome', 'No Name')
                    print(f"  > Processing Product ID: {product_id} ('{product_name}')")
                    
                    # 1. Add Real Compatibility Data
                    compatibility_list = product_compatibility.get(product_id, [])
                    compatibility_string = format_compatibility_string(compatibility_list)
                    record['compatibilita'] = compatibility_string
                    
                    if compatibility_string:
                        print(f"    ✅ Real compatibility: {compatibility_string}")
                    else:
                        print(f"    ⚠️  No compatibility data found")
                    
                    # 2. Process and Download Gallery Images (same as before)
                    company_code = record.get('immagine', {}).get('codiceAzienda') if isinstance(record.get('immagine'), dict) else None
                    gallery_filenames = []
                    if company_code and isinstance(record.get('galleria'), str):
                        company_folder = os.path.join(IMAGE_DIRECTORY, company_code)
                        try:
                            gallery_items = json.loads(record['galleria'])
                            for item in gallery_items:
                                img_name = item.get('immagine')
                                if img_name:
                                    gallery_filenames.append(img_name)
                                    img_url = f"{gallery_image_base_url}{company_code}/{img_name}"
                                    download_image(img_url, company_folder, session)
                        except (json.JSONDecodeError, AttributeError):
                            pass
                    record['gallery_filenames'] = ", ".join(gallery_filenames)
                    
                    all_products_data.append(record)
                
            except requests.exceptions.RequestException as e:
                print(f"FATAL ERROR on page {page_num}: {e}. Stopping.")
                break
            
            time.sleep(random.uniform(1.0, 2.5))  # Be polite to the server
    
    # 5. Write all collected data to CSV
    if all_products_data:
        print(f"\n💾 Writing {len(all_products_data)} products to '{OUTPUT_CSV_FILE}'...")
        
        try:
            fieldnames = list(all_products_data[0].keys())
            with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(all_products_data)
            
            print("✅ Successfully created CSV file with REAL compatibility data!")
            
            # Show statistics
            products_with_compatibility = sum(1 for p in all_products_data if p.get('compatibilita'))
            print(f"\n📊 FINAL STATISTICS:")
            print(f"   Total products: {len(all_products_data)}")
            print(f"   Products with compatibility: {products_with_compatibility}")
            print(f"   Products without compatibility: {len(all_products_data) - products_with_compatibility}")
            print(f"   Coverage: {products_with_compatibility/len(all_products_data)*100:.1f}%")
            
        except IOError as e:
            print(f"Could not write to file {OUTPUT_CSV_FILE}. Error: {e}")
    else:
        print("No data was scraped. The output file was not created.")

if __name__ == '__main__':
    print("🎯 COMPLETE COMPATIBILITY SOLUTION")
    print("This script will extract REAL compatibility data using the relationship table!")
    print()
    
    response = input("Do you want to proceed with the complete compatibility extraction? (y/n): ")
    if response.lower() in ['y', 'yes']:
        scrape_products_with_real_compatibility()
    else:
        print("Extraction cancelled.")
