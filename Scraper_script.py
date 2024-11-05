import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from urllib.parse import urljoin
import logging

class BusinessListScraper:
    def __init__(self):
        self.base_url = "https://www.businesslist.com.ng/category/small-business"
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        # Setup detailed logging
        logging.basicConfig(
            filename='scraper.log',
            level=logging.DEBUG,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def get_soup(self, url, retries=3):
        """Get BeautifulSoup object with retry mechanism"""
        self.logger.debug(f"Attempting to fetch URL: {url}")
        
        for i in range(retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                self.logger.debug(f"Successfully fetched URL: {url}")
                return BeautifulSoup(response.text, 'html.parser')
            except Exception as e:
                self.logger.error(f"Attempt {i+1} failed for URL {url}: {str(e)}")
                if i == retries - 1:
                    raise
                time.sleep(2 ** i)

    def extract_business_links(self, page_url):
        """Extract business links from a given page URL"""
        try:
            self.logger.info(f"Starting to extract business links from {page_url}")
            soup = self.get_soup(page_url)
            
            # Find all company divs with the correct class pattern
            company_divs = soup.find_all('div', class_=lambda x: x and 'company with_img g_' in x)
            self.logger.debug(f"Found {len(company_divs)} company divs")
            
            links = []
            for company in company_divs:
                # Find the link within h4 tag
                link_elem = company.find('h4').find('a') if company.find('h4') else None
                if link_elem and 'href' in link_elem.attrs:
                    full_url = urljoin(self.base_url, link_elem['href'])
                    links.append(full_url)
                    self.logger.debug(f"Found business link: {full_url}")
            
            return links
                
        except Exception as e:
            self.logger.error(f"Error extracting business links: {str(e)}")
            return []
    
    def scrape_business_details(self, url):
        """Scrape details from individual business page"""
        try:
            soup = self.get_soup(url)
            
            business_data = {
                'Company Name': None,
                'Location': None,
                'Phone Number': None,
                'Website URL': None,
                'Company Size': None,      
                'Primary Contact Name': None, 
                'Contact Position':'Company Manager',
                'Contact Source': 'BusinessList.com.ng',
                 # Added field for Company Manager
            }

            # Company Name from h1
            name_elem = soup.find('h1')
            if name_elem:
                business_data['Company Name'] = name_elem.text.strip().split(' - ')[0]
                self.logger.debug(f"Found company name: {business_data['Company Name']}")

            # Location - extract text from the div with id "company_address"
            location_div = soup.find('div', id='company_address')
            if location_div:
                business_data['Location'] = location_div.text.strip()
                self.logger.debug(f"Found location: {business_data['Location']}")

            # Contact number
            contact_div = soup.find('div', string=lambda x: x and 'Contact number' in str(x))
            if contact_div and contact_div.find_next('div'):
                business_data['Phone Number'] = contact_div.find_next('div').text.strip()
                self.logger.debug(f"Found phone: {business_data['Phone Number']}")

            # Mobile phone as alternate
            mobile_div = soup.find('div', string=lambda x: x and 'Mobile phone' in str(x))
            if mobile_div and mobile_div.find_next('div'):
                if not business_data['Phone Number']:
                    business_data['Phone Number'] = mobile_div.find_next('div').text.strip()

            # Website Address
            website_div = soup.find('div', string=lambda x: x and 'Website' in str(x))
            if website_div and website_div.find_next('div'):
                business_data['Website URL'] = website_div.find_next('div').text.strip()
                self.logger.debug(f"Found website address: {business_data['Website URL']}")

            # Company Size
            try:
                employees_div = soup.find('span', class_='label', string='Employees')
                if employees_div and employees_div.parent:
                    # Get the text after the "Company Size" label, which contains the count
                    employees_text = employees_div.parent.get_text(strip=True)
                    # Extract just the count part (e.g., "1-5") by removing "Company Size"
                    employees_count = employees_text.replace('Employees', '').strip()
                    
                    # Determine company size category
                    if '-' in employees_count:
                        size_range = employees_count.split('-')
                        if len(size_range) == 2:
                            lower_bound = int(size_range[0])
                            upper_bound = int(size_range[1])
                            if upper_bound <= 50:
                                business_data['Company Size'] = 'Small'
                            elif 100 <= lower_bound <= 500:
                                business_data['Company Size'] = 'Medium'
                            elif lower_bound > 500:
                                business_data['Company Size'] = 'Large'
                            else:
                                business_data['Company Size'] = 'Unknown'
                        else:
                            business_data['Company Size'] = 'Unknown'
                    else:
                        # Handle single number cases
                        if int(employees_count) <= 50:
                            business_data['Company Size'] = 'Small'
                        elif 100 <= int(employees_count) <= 500:
                            business_data['Company Size'] = 'Medium'
                        elif int(employees_count) > 500:
                            business_data['Company Size'] = 'Large'
                        else:
                            business_data['Company Size'] = 'Unknown'
                    
                    self.logger.debug(f"Found employees: {business_data['Company Size']}")
            except Exception as e:
                self.logger.error(f"Error extracting employees count: {str(e)}")
                business_data['Company Size'] = None

            # Primary Contact Name
            try:
                info_divs = soup.find_all('div', class_='info')
                for div in info_divs:
                    manager_label = div.find('span', class_='label', string='Company manager')
                    if manager_label:
                        # Get the text content after the span, which contains the manager's name
                        manager_text = div.get_text(strip=True)
                        # Remove the "Company manager" text to get just the name
                        manager_name = manager_text.replace('Company manager', '').strip()
                        business_data['Primary Contact Name'] = manager_name
                        self.logger.debug(f"Found company manager: {business_data['Primary Contact Name']}")
                        break
            except Exception as e:
                self.logger.error(f"Error extracting company manager: {str(e)}")
                business_data['Primary Contact Name'] = None

            # Add debug logging
            self.logger.debug(f"Scraped data for {url}: {business_data}")
            
            return business_data

        except Exception as e:
            self.logger.error(f"Error scraping business details from {url}: {str(e)}")
            return None


    def scrape_first_page(self):
        """Scrape the first 15 pages of businesses"""
        self.logger.info("Starting to scrape first 15 pages")
        all_businesses = []
        
        for page in range(1, 16): 
            page_url = f"{self.base_url}/{page}"  # Construct the URL for each page
            self.logger.info(f"Scraping page {page}: {page_url}")
            
            # Get business links from the current page
            business_links = self.extract_business_links(page_url)
            print(f"Found {len(business_links)} business links on page {page}:")
            for link in business_links:
                print(f"- {link}")
            
            # Scrape each business
            for link in business_links:
                try:
                    print(f"\nScraping: {link}")
                    business_data = self.scrape_business_details(link)
                    if business_data:
                        print(f"Scraped data: {business_data}")
                        all_businesses.append(business_data)
                    time.sleep(2)  # Sleep between requests to avoid hitting the website too hard
                except Exception as e:
                    print(f"Error processing {link}: {str(e)}")
                    continue
            
            # After every 5 pages, save to Google Sheets to reduce write requests
            if page % 5 == 0:
                self.save_to_google_sheet(all_businesses)
                all_businesses = []  # Clear the list after saving

        # Save any remaining businesses after the loop
        if all_businesses:
            self.save_to_google_sheet(all_businesses)

    def save_to_google_sheet(self, businesses):
        """Save scraped data to Google Sheets"""
        # Define the scope and authenticate
        scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        creds = ServiceAccountCredentials.from_json_keyfile_name(r'C:\Users\ayo\Webscraping\elegant-moment-413814-6e8f42efa6fc.json', scope)
        client = gspread.authorize(creds)

        # Open the Google Sheet
        sheet = client.open("ShopMammy Tracker").sheet1 

        # Write header only if the sheet is empty
        if not sheet.get_all_values():  # Check if the sheet is empty
            header = ['Company Name', 'Location', 'Phone Number', 'Website URL', 'Company Size', 'Primary Contact Name', 'Contact Position', 'Contact Source']
            sheet.append_row(header)

        # Prepare data for batch writing
        rows_to_write = []
        for business in businesses:
            row = [
                business['Company Name'],
                business['Location'],
                business['Phone Number'],
                business['Website URL'],
                business['Company Size'],
                business['Primary Contact Name'],
                business['Contact Position'],
                business['Contact Source']
            ]
            rows_to_write.append(row)

        # Write all data at once
        if rows_to_write:
            sheet.append_rows(rows_to_write)  # Use append_rows for batch writing

        print("Data successfully saved to Google Sheets.")
def main():
    # Initialize scraper
    scraper = BusinessListScraper()
    
    try:
        # Start scraping only first page
        print("Starting scrape of first page...")
        businesses = scraper.scrape_first_page()
        
        # Print results before saving
        print(f"Found {len(businesses)} businesses")
        
        # Save results to Google Sheets
        if businesses:
            scraper.save_to_google_sheet(businesses)  # Call the new method to save to Google Sheets
        else:
            print("No businesses found to save!")
        
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        logging.error(f"Critical error in main: {str(e)}")

if __name__ == "__main__":
    main()