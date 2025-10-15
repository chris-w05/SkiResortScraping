import requests
from bs4 import BeautifulSoup
import math
import concurrent.futures
import csv
import re
import sqlite3
import json
from datetime import datetime

def extract_resorts(soup):
    resort_divs = soup.find_all('div', class_='panel panel-default resort-list-item resort-list-item-image--big')
    
    resorts = []
    for div in resort_divs:
        a_tag = div.find('a', class_='h3')
        if a_tag:
            name = a_tag.text.strip()
            link = a_tag['href']
            if not link.startswith('http'):
                link = "https://www.skiresort.info" + link
            
            # Extract location from breadcrumb
            breadcrumb_div = div.find('div', class_='sub-breadcrumb')
            locations = []
            if breadcrumb_div:
                inner_divs = [child for child in breadcrumb_div.children 
                              if hasattr(child, 'name') and child.name == 'div' and 'sub-breadcrumb' in child.get('class', [])]
                
                if not inner_divs:
                    loc = ' > '.join([a.text.strip() for a in breadcrumb_div.find_all('a')])
                    if loc:
                        locations.append(loc)
                else:
                    for inner_div in inner_divs:
                        loc = ' > '.join([a.text.strip() for a in inner_div.find_all('a')])
                        if loc:
                            locations.append(loc)
            
            location = ' | '.join(locations)
            
            # Extract additional info from info-table
            info_table = div.find('table', class_='info-table')
            rating = elev_diff = min_alt = max_alt = total_km = easy_km = inter_km = diff_km = num_lifts = price = ''
            if info_table:
                rows = info_table.find_all('tr')
                if len(rows) >= 5:
                    # Rating
                    star_div = rows[0].find('div', class_='js-star-ranking')
                    rating = star_div['data-rank'] if star_div else ''
                    
                    # Height
                    height_td = rows[1].find_all('td')[1] if len(rows[1].find_all('td')) > 1 else None
                    if height_td:
                        height_spans = height_td.find_all('span')
                        if len(height_spans) == 3:
                            elev_diff = height_spans[0].text.strip()
                            min_alt = height_spans[1].text.strip().strip('()')
                            max_alt = height_spans[2].text.strip().strip('()')
                    
                    # Slopes
                    slope_td = rows[2].find_all('td')[1] if len(rows[2].find_all('td')) > 1 else None
                    if slope_td:
                        slope_spans = slope_td.find_all('span', class_='slopeinfoitem')
                        if len(slope_spans) >= 4:
                            total_km = slope_spans[0].text.strip()
                            easy_km = slope_spans[1].text.strip()
                            inter_km = slope_spans[2].text.strip()
                            diff_km = slope_spans[3].text.strip()
                    
                    # Lifts
                    lifts_td = rows[3].find_all('td')[1] if len(rows[3].find_all('td')) > 1 else None
                    if lifts_td:
                        li = lifts_td.find('li')
                        if li:
                            num_lifts = li.text.split('\xa0')[0]
                    
                    # Price
                    price_td = rows[4].find_all('td')[1] if len(rows[4].find_all('td')) > 1 else None
                    price = price_td.text.strip() if price_td else ''
            
            resorts.append({
                'name': name,
                'link': link,
                'location': location,
                'rating': rating,
                'elev_diff': elev_diff,
                'min_alt': min_alt,
                'max_alt': max_alt,
                'total_km': total_km,
                'easy_km': easy_km,
                'inter_km': inter_km,
                'diff_km': diff_km,
                'num_lifts': num_lifts,
                'price': price
            })
    
    return resorts

# Fetch page 1 to get total pages
url = "https://www.skiresort.info/ski-resorts/"
response = requests.get(url)
if response.status_code == 200:
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extract resorts from page 1
    all_resorts = extract_resorts(soup)
    print(f"Fetched page 1 with {len(all_resorts)} resorts")
    
    # Find total resorts to calculate total pages
    result_text = soup.find(string=re.compile(r"\d+ - \d+ out of \d+"))
    total_resorts = None
    per_page = None
    if result_text:
        parts = re.findall(r'\d+', result_text)
        if len(parts) >= 3:
            start = int(parts[0])
            end = int(parts[1])
            total_resorts = int(parts[2])
            per_page = end - start + 1
    
    if total_resorts and per_page:
        total_pages = math.ceil(total_resorts / per_page)
    else:
        total_pages = 32  # Fallback if unable to parse
else:
    all_resorts = []
    total_pages = 0

# Function to fetch a single page's text
def fetch_page(page):
    url = f"https://www.skiresort.info/ski-resorts/page/{page}/"
    response = requests.get(url)
    return response.text if response.status_code == 200 else None

# Fetch remaining pages concurrently
if total_pages > 1:
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_page = {executor.submit(fetch_page, page): page for page in range(2, total_pages + 1)}
        for future in concurrent.futures.as_completed(future_to_page):
            page = future_to_page[future]
            try:
                page_text = future.result()
                if page_text:
                    soup = BeautifulSoup(page_text, 'html.parser')
                    resorts = extract_resorts(soup)
                    all_resorts.extend(resorts)
                    print(f"Fetched page {page} with {len(resorts)} resorts")
            except Exception as e:
                print(f"Error fetching page {page}: {e}")

# Now fetch details for each resort concurrently
def fetch_details(link):
    try:
        response = requests.get(link)
        if response.status_code != 200:
            return {'current_season': '', 'general_season': '', 'opening_times': ''}
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        current_season = ''
        season_td = soup.find('td', id='selSeason')
        if season_td:
            current_season = season_td.text.strip()
        
        general_season = ''
        gen_td = soup.find('td', id='selGenseason')
        if gen_td:
            general_season = ' '.join(gen_td.text.strip().split())
        
        opening_times = ''
        op_td = soup.find('td', id='selOperationtimes')
        if op_td:
            opening_times = op_td.text.strip()
        
        return {
            'current_season': current_season,
            'general_season': general_season,
            'opening_times': opening_times
        }
    except Exception as e:
        print(f"Error fetching details for {link}: {e}")
        return {'current_season': '', 'general_season': '', 'opening_times': ''}

# Concurrently fetch details
with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
    future_to_resort = {executor.submit(fetch_details, resort['link']): resort for resort in all_resorts}
    for future in concurrent.futures.as_completed(future_to_resort):
        resort = future_to_resort[future]
        try:
            details = future.result()
            resort.update(details)
        except Exception as e:
            print(f"Error updating resort {resort['name']}: {e}")

# Parse general_season to approx dates
months = {
    'January': 1, 'February': 2, 'March': 3, 'April': 4, 'May': 5, 'June': 6,
    'July': 7, 'August': 8, 'September': 9, 'October': 10, 'November': 11, 'December': 12
}
qual_days = {
    'early': 7, 'mid': 15, 'late': 23, 'beginning': 1, 'end': 28,
    'beginning of': 1, 'end of': 28
}

def parse_season(season_str):
    if not season_str or '-' not in season_str:
        return '', ''
    
    parts = [p.strip() for p in season_str.split('-') if p.strip()]
    if len(parts) != 2:
        return '', ''
    
    start, end = parts
    
    def get_month_day(s):
        words = s.split()
        qual_str = ''
        month_str = ''
        for word in words:
            w_lower = word.lower()
            if w_lower in qual_days or w_lower + ' of' in qual_days or 'of' in w_lower:
                qual_str += ' ' + w_lower
            elif word.capitalize() in months:
                month_str = word.capitalize()
                break
        qual_str = qual_str.strip()
        day = qual_days.get(qual_str, 15)
        month_num = months.get(month_str, None)
        return month_num, day
    
    start_month, start_day = get_month_day(start)
    end_month, end_day = get_month_day(end)
    
    if start_month is None or end_month is None:
        return '', ''
    
    approx_start = f"{start_month:02d}-{start_day:02d}"
    approx_end = f"{end_month:02d}-{end_day:02d}"
    return approx_start, approx_end

for resort in all_resorts:
    approx_start, approx_end = parse_season(resort['general_season'])
    resort['approx_season_start'] = approx_start
    resort['approx_season_end'] = approx_end

# Normalize prices to USD
currency_symbols = {
    '€': 'EUR', '$': 'USD', 'US$': 'USD', 'CHF': 'CHF', 'CAD': 'CAD', '¥': 'JPY',
    '£': 'GBP', 'SFr.': 'CHF', 'C$':'CFA', 'NOK':'NOK', 'Skr':'SEK', 'NZ$':'NZD', 'BGN':'BGN',
    'RSD':'RSD'
    #Add more as needed
}

def parse_price(price_str):
    price_str = price_str.strip()
    if not price_str:
        return '', 0.0
    
    # Find currency
    curr_sym = price_str[0] if price_str[0] in currency_symbols else price_str.split()[0]
    curr = currency_symbols.get(curr_sym, '')
    
    # Find number
    num_match = re.search(r'(\d+)[.,]?(-|\d{1,2})?', price_str)
    if num_match:
        whole = num_match.group(1)
        dec = num_match.group(2) or '00'
        dec = '00' if dec == '-' else dec.zfill(2)
        value_str = whole + '.' + dec
        try:
            value = float(value_str)
            return curr, value
        except:
            pass
    # Fallback to digits only
    digits = re.findall(r'\d+', price_str)
    if digits:
        value_str = '.'.join(digits[:2]) if len(digits) > 1 else digits[0]
        return curr, float(value_str)
    return '', 0.0

# Collect unique currencies
currencies = set()
for resort in all_resorts:
    curr, _ = parse_price(resort['price'])
    if curr and curr != 'USD':
        currencies.add(curr)

# Fetch exchange rates (to USD)
def get_exchange_rate(from_curr):
    if from_curr == 'USD':
        return 1.0
    url = f"https://api.frankfurter.app/latest?from={from_curr}&to=USD"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            return data['rates']['USD']
    except:
        pass
    return None

rates = {'USD': 1.0}
for curr in currencies:
    rate = get_exchange_rate(curr)
    if rate:
        rates[curr] = rate
    else:
        rates[curr] = 0.0  # Error, no conversion
        print(f"Failed to fetch rate for {curr}")

# Apply conversions
for resort in all_resorts:
    orig_price = resort['price']
    curr, value = parse_price(orig_price)
    resort['original_currency'] = curr
    resort['original_value'] = value
    resort['exchange_rate'] = rates.get(curr, 0.0)
    resort['usd_price'] = value * resort['exchange_rate'] if curr else 0.0

# Split location into continent, country, region, locality
for resort in all_resorts:
    continents = set()
    countries = set()
    regions = []
    localities = []
    paths = resort['location'].split(' | ')
    for path in paths:
        parts = [p.strip() for p in path.split(' > ') if p.strip()]
        if len(parts) >= 1:
            continents.add(parts[0])
        if len(parts) >= 2:
            countries.add(parts[1])
        if len(parts) >= 3:
            regions.append(' > '.join(parts[2:-1]))
        if len(parts) >= 4:
            localities.append(parts[-1])
    
    resort['continent'] = ' | '.join(continents)
    resort['country'] = ' | '.join(countries)
    resort['region'] = ' | '.join(regions) if regions else ''
    resort['locality'] = ' | '.join(localities) if localities else ''

# Save to CSV
fieldnames = [
    'name', 'link', 'location', 'continent', 'country', 'region', 'locality', 'rating', 'elev_diff', 'min_alt', 'max_alt',
    'total_km', 'easy_km', 'inter_km', 'diff_km', 'num_lifts', 'price',
    'current_season', 'general_season', 'opening_times',
    'approx_season_start', 'approx_season_end',
    'original_currency', 'original_value', 'exchange_rate', 'usd_price'
]

with open('comprehensive_ski_resorts.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_resorts)

# Save to SQLite database
conn = sqlite3.connect('ski_resorts.db')
cur = conn.cursor()

# Create table
create_query = '''
CREATE TABLE IF NOT EXISTS resorts (
    name TEXT,
    link TEXT,
    location TEXT,
    continent TEXT,
    country TEXT,
    region TEXT,
    locality TEXT,
    rating TEXT,
    elev_diff TEXT,
    min_alt TEXT,
    max_alt TEXT,
    total_km TEXT,
    easy_km TEXT,
    inter_km TEXT,
    diff_km TEXT,
    num_lifts TEXT,
    price TEXT,
    current_season TEXT,
    general_season TEXT,
    opening_times TEXT,
    approx_season_start TEXT,
    approx_season_end TEXT,
    original_currency TEXT,
    original_value REAL,
    exchange_rate REAL,
    usd_price REAL
)
'''
cur.execute(create_query)

# Insert data
for resort in all_resorts:
    cur.execute('''
    INSERT INTO resorts VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    ''', [resort.get(field, '') for field in fieldnames])

conn.commit()
conn.close()

print(f"Total resorts found: {len(all_resorts)}")
print("Data saved to comprehensive_ski_resorts.csv and ski_resorts.db")