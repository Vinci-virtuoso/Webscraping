import gspread
from oauth2client.service_account import ServiceAccountCredentials
from geopy.distance import geodesic
from geopy.geocoders import Nominatim

def get_coordinates(location, retries=3):
    """Convert a location string to latitude and longitude with retry logic."""
    geolocator = Nominatim(user_agent="business_locator")
    for attempt in range(retries):
        try:
            location_data = geolocator.geocode(location)
            if location_data:
                return (location_data.latitude, location_data.longitude)
            else:
                print(f"Location not found for: {location}")
                log_failed_geocoding(location)  # Log the failed attempt
                return None  # Location not found
        except Exception as e:
            print(f"Error geocoding location '{location}': {str(e)}")
            if attempt < retries - 1:
                print("Retrying...")
            else:
                return None

def preprocess_address(address):
    """Extract the town from the address to improve geocoding results."""
    # Split the address and extract the town
    parts = address.split(',')
    if len(parts) > 1:
        town = parts[-2].strip()  # Assuming the town is the second last part
        return town
    return address  # Return the original address if no town is found  

def log_failed_geocoding(location):
    with open("failed_geocoding.log", "a") as log_file:
        log_file.write(f"Failed to geocode: {location}\n")        

# Define the coordinates for the universities
universities = {
    "UNILAG": (6.517912, 3.385983),  # University of Lagos
    "UI": (7.44167, 3.90000),      # University of Ibadan
    "Caleb University": (6.6113, 3.8234),  # Caleb University
    "LASUTH": (6.5644, 3.3470)   # Lagos State University Teaching Hospital
}

def is_within_distance(business_location, universities, max_distance=5):
    """Check if the business is within max_distance miles of any university."""
    for uni, coords in universities.items():
        distance = geodesic(business_location, coords).miles
        if distance <= max_distance:
            return True
    return False

def qualify_leads():
    # Define the scope and authenticate
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(r'C:\Users\ayo\Webscraping\elegant-moment-413814-6e8f42efa6fc.json', scope)
    client = gspread.authorize(creds)

    # Open the existing Google Sheet
    existing_sheet = client.open("ShopMammy Tracker").sheet1
    existing_data = existing_sheet.get_all_records()

    print("Existing data retrieved from Google Sheet:")
    print(existing_data)

    # Open the existing Google Sheet and access sheet 2 for qualified leads
    qualified_sheet = client.open("ShopMammy Tracker").get_worksheet(1)  # Accessing the second sheet (index 1)
    qualified_header = ['Company Name', 'Location', 'State', 'Phone Number', 'Website URL', 'Company Size', 'Primary Contact Name', 'Contact Position', 'Contact Source', 'Proximity Qualification']
    qualified_sheet.append_row(qualified_header)

    # Process each business and qualify leads
    for business in existing_data:
        # Extract relevant data
        company_name = business.get('Company Name')
        location = business.get('Location')
        state = business.get('State')
        phone_number = business.get('Contact Phone Number')
        website_url = business.get('Website')
        company_size = business.get('Company Size')
        primary_contact_name = business.get('Contact Person Name')
        contact_position = business.get('Contact Person Position')
        contact_source = business.get('Contact Source')

        # Validate location
        if not location:
            print("Location is missing for a business. Skipping...")
            continue  # Skip this business if location is not found

        # Extract the town from the location
        town = preprocess_address(location)

        # Get coordinates from the location
        business_location = get_coordinates(town)
        if not business_location:
            print(f"Could not get coordinates for location: {town}")
            continue  # Skip this business if coordinates are not found

        # Print the coordinates for debugging
        print(f"Coordinates for {location}: {business_location}")

        # Check proximity and state criteria
        proximity_qualification = 'Not Qualified'
        if is_within_distance(business_location, universities):
            proximity_qualification = 'Within 5 miles of a university'

        # You can also add state qualification logic here
        if state in ['Lagos', 'Oyo']:  # Example states to qualify
            proximity_qualification += ' and in a qualified state'

        # Append to the new sheet if qualified
        if proximity_qualification != 'Not Qualified':
            qualified_row = [
                company_name,
                location,
                state,
                phone_number,
                website_url,
                company_size,
                primary_contact_name,
                contact_position,
                contact_source,
                proximity_qualification
            ]
            qualified_sheet.append_row(qualified_row)
            print(f"Qualified lead added: {qualified_row}")

    print("Qualified leads have been populated in the new sheet.")

if __name__ == "__main__":
    qualify_leads()