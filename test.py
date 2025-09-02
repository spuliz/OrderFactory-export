import requests
import json
import csv
import time
import random
import os

# ==============================================================================
# SCRIPT CONFIGURATION
# ==============================================================================

TOTAL_PAGES = 148  # There are 148 pages of products
PAGE_SIZE = 20
OUTPUT_CSV_FILE = 'all_products_with_compatibility.csv'
IMAGE_DIRECTORY = 'images'

# --- !!! IMPORTANT: UPDATE THIS COOKIE VALUE !!! ---
# Follow the instructions above to get your current cookie from the browser.
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
# ==============================================================================


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


def get_product_details(product_id, session):
    """Fetches detailed JSON for a single product to extract compatibility."""
    if not product_id:
        return ""

    detail_url = f'https://merchant.matchingnumber.com/eng/gestione/getform/class/Dati_Prodotto?id={product_id}'
    try:
        response = session.get(detail_url)
        response.raise_for_status()
        data = response.json()

        compatibility_list = []
        for item in data.get('voices', []):
            if item.get('id') == 'compatibilita':
                for vehicle in item.get('init', []):
                    brand = vehicle.get('marchio', 'N/A')
                    model = vehicle.get('modello', 'N/A')
                    from_year = vehicle.get('da_anno_modello', '')
                    to_year = vehicle.get('a_anno_modello', '')
                    year_range = f"({from_year}-{to_year})" if from_year else ""
                    compatibility_list.append(f"{brand} {model} {year_range}".strip())
                break
        
        if compatibility_list:
            print(f"    \u2713 Found compatibility for {len(compatibility_list)} vehicle(s).")
            return "; ".join(compatibility_list)
        else:
            print("    - No compatibility info found.")
            return ""
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        print(f"    \u2717 Error fetching details for product {product_id}. {e}")
        return ""


def scrape_all_products():
    """
    Main function to scrape all products, get details, download images, and save to CSV.
    """
    base_url = 'https://merchant.matchingnumber.com/eng/gestione/getlist/class/Dati_Prodotto'
    gallery_image_base_url = 'https://merchant.matchingnumber.com/scripts/imgresize.php?pic=immagini/articoli/'
    
    total_products = TOTAL_PAGES * PAGE_SIZE
    print(f"Starting scrape for approximately {total_products} products across {TOTAL_PAGES} pages.")
    os.makedirs(IMAGE_DIRECTORY, exist_ok=True)

    all_products_data = []

    with requests.Session() as session:
        session.headers.update(HEADERS)

        for page_num in range(1, TOTAL_PAGES + 1):
            print(f"\n--- Scraping Page {page_num}/{TOTAL_PAGES} ---")
            list_params = {'rand': [random.randint(1000, 9999), random.randint(1000, 9999)]}
            list_data = {'pageNo': page_num, 'pageSize': PAGE_SIZE, 'sort': '', 'dir': '', 'session': 'false', 'sessionId': ''}

            try:
                response = session.post(base_url, params=list_params, data=list_data, timeout=20)
                response.raise_for_status()
                records = response.json().get('records', [])

                if not records:
                    print("No more records found on this page. Stopping.")
                    break
                
                for record in records:
                    product_id = record.get('id')
                    product_name = record.get('nome', 'No Name')
                    print(f"  > Processing Product ID: {product_id} ('{product_name}')")

                    # 1. Get Compatibility Details
                    record['compatibilita'] = get_product_details(product_id, session)

                    # 2. Process and Download Gallery Images
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
            
            time.sleep(random.uniform(1.0, 2.5)) # Be polite to the server

    # 3. Write all collected data to a single CSV file at the end
    if all_products_data:
        print(f"\nScraping finished. Writing {len(all_products_data)} records to '{OUTPUT_CSV_FILE}'...")
        try:
            fieldnames = list(all_products_data[0].keys())
            with open(OUTPUT_CSV_FILE, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                writer.writerows(all_products_data)
            print("Successfully created CSV file!")
        except IOError as e:
            print(f"Could not write to file {OUTPUT_CSV_FILE}. Error: {e}")
    else:
        print("No data was scraped. The output file was not created.")


if __name__ == '__main__':
    scrape_all_products()