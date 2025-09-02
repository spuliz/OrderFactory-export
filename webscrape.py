import requests
import json
import csv
import time
import random
import os

def download_image(url, folder_path, session):
    """
    Downloads an image from a given URL and saves it to a folder if it doesn't exist.
    This version includes a check to ensure the content is an image.
    """
    if not url:
        return
    try:
        filename = os.path.join(folder_path, url.split('/')[-1])
        
        if os.path.exists(filename):
            return

        # Use a context manager and stream=True to handle the download efficiently
        with session.get(url, stream=True) as response:
            response.raise_for_status()

            # --- BUG FIX: Verify that the response is actually an image ---
            content_type = response.headers.get('content-type')
            if not content_type or 'image' not in content_type.lower():
                print(f"Skipping download for {url}. Expected an image but got Content-Type: {content_type}")
                return
            # --- END OF FIX ---

            # Save the image to the specified folder
            with open(filename, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

    except requests.exceptions.RequestException as e:
        print(f"Failed to download {url}. Error: {e}")
    except IOError as e:
        print(f"Failed to save image {filename}. Error: {e}")


def scrape_products_corrected():
    """
    Scrapes product data, saves it to a CSV file with image filenames,
    and downloads the images to a local folder for ERP import.
    """
    # Base URLs
    base_url = 'https://merchant.matchingnumber.com/eng/gestione/getlist/class/Dati_Prodotto'
    image_base_url = 'https://www.matchingnumber.com/media/immagini/'

    # Headers
    headers = {
        'accept': 'application/json', 'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'cookie': 'PHPSESSID=1f415a6d1de0065c189225ba3c2c0da9; osbid=3; osbk=77eae7a9e5dfe7c670e76ce054f78c6740245f15',
        'origin': 'https://merchant.matchingnumber.com', 'priority': 'u=1, i',
        'referer': 'https://merchant.matchingnumber.com/eng/gestione/index?class=Dati_Prodotto',
        'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
        'sec-ch-ua-mobile': '?0', 'sec-ch-ua-platform': '"macOS"', 'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors', 'sec-fetch-site': 'same-origin',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
        'x-request': 'JSON', 'x-requested-with': 'XMLHttpRequest',
    }

    total_pages = 147
    page_size = 20
    output_file = 'products.csv'
    image_folder = 'images'

    if not os.path.exists(image_folder):
        os.makedirs(image_folder)
        print(f"Created directory: {image_folder}")
    
    print("Starting to scrape product data and download images...")

    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            csv_writer = None
            with requests.Session() as session:
                session.headers.update(headers)

                for page_num in range(1, total_pages + 1):
                    params = {'rand': [random.randint(100, 9999), random.randint(100, 9999)]}
                    data = {'pageNo': page_num, 'pageSize': page_size, 'sort': '', 'dir': '', 'session': 'false', 'sessionId': ''}

                    try:
                        response = session.post(base_url, params=params, data=data)
                        response.raise_for_status()
                        records = response.json().get('records', [])
                        
                        if not records:
                            print(f"No more records found on page {page_num}. Stopping.")
                            break
                        
                        processed_records = []
                        for record in records:
                            # Extract company code BEFORE modifying the record
                            company_code = None
                            if isinstance(record.get('immagine'), dict):
                                company_code = record['immagine'].get('codiceAzienda')

                            # Process the main image
                            main_image_filename = ""
                            if company_code and isinstance(record.get('immagine'), dict):
                                image_name = record['immagine'].get('immagine')
                                if image_name:
                                    main_image_url = f"{image_base_url}{company_code}/{image_name}"
                                    main_image_filename = image_name
                                    download_image(main_image_url, image_folder, session)
                            record['immagine'] = main_image_filename # Replace dict with filename string

                            # Process the gallery images
                            gallery_filenames = []
                            # Use the stored company_code
                            if company_code and isinstance(record.get('galleria'), str):
                                try:
                                    gallery_items = json.loads(record['galleria'])
                                    for item in gallery_items:
                                        gallery_image_name = item.get('immagine')
                                        if gallery_image_name:
                                            gallery_filenames.append(gallery_image_name)
                                            gallery_url = f"{image_base_url}{company_code}/{gallery_image_name}"
                                            download_image(gallery_url, image_folder, session)
                                except (json.JSONDecodeError, AttributeError):
                                    pass
                            # Replace JSON string with a comma-separated list of filenames
                            record['galleria'] = ",".join(gallery_filenames)
                            
                            processed_records.append(record)

                        # Initialize CSV writer with headers from the first processed record
                        if csv_writer is None and processed_records:
                            fieldnames = processed_records[0].keys()
                            csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                            csv_writer.writeheader()

                        # Write the processed records to the CSV
                        for record in processed_records:
                            csv_writer.writerow(record)

                        print(f"Successfully processed page {page_num}/{total_pages}.")

                    except requests.exceptions.RequestException as e:
                        print(f"An error occurred on page {page_num}: {e}")
                        break
                    except json.JSONDecodeError:
                        print(f"Failed to decode JSON on page {page_num}. Response: {response.text}")
                        break
                    
                    time.sleep(1)

    except IOError as e:
        print(f"An error occurred while writing to the file {output_file}: {e}")
    
    print(f"\nScraping complete. Data saved to {output_file} and images to '{image_folder}' folder.")


if __name__ == '__main__':
    scrape_products_corrected()
