from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import csv
import time
from datetime import datetime
import sys
import traceback
import logging
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('dhan_scraper.log'),
        logging.StreamHandler()
    ]
)

class DhanStockScraper:
    def __init__(self):
        self.url = "https://dhan.co/all-stocks-list/"
        self.csv_file = "dhan_stocks.csv"
        self.driver = None
        
        # Email configuration - Use environment variables for security
        self.email_config = {
            'smtp_server': 'smtp.gmail.com',
            'smtp_port': 587,
            'sender_email': os.getenv('SENDER_EMAIL'),
            'sender_password': os.getenv('EMAIL_PASSWORD'),
            'recipient_email': os.getenv('RECIPIENT_EMAIL')
        }
        
    def setup_driver(self):
        """Setup Chrome WebDriver"""
        logging.info("Setting up Chrome WebDriver...")
        chrome_options = Options()
        
        # Heroku Chrome buildpack options
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--remote-debugging-port=9222")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        
        # Check if running on Heroku
        if 'DYNO' in os.environ:
            chrome_options.binary_location = os.getenv('GOOGLE_CHROME_BIN')
        
        try:
            if 'DYNO' in os.environ:
                # Use Heroku Chrome binary
                self.driver = webdriver.Chrome(
                    executable_path=os.getenv('CHROMEDRIVER_PATH'),
                    options=chrome_options
                )
            else:
                # Local development setup
                service = Service(ChromeDriverManager().install())
                self.driver = webdriver.Chrome(service=service, options=chrome_options)
            
            self.driver.implicitly_wait(10)
            logging.info("Chrome WebDriver setup successful")
        except Exception as e:
            logging.error(f"Failed to setup Chrome WebDriver: {str(e)}")
            raise
        
    def cleanup_driver(self):
        """Clean up WebDriver resources"""
        if self.driver:
            try:
                self.driver.quit()
                logging.info("WebDriver cleaned up")
            except Exception as e:
                logging.error(f"Error cleaning up WebDriver: {str(e)}")
            finally:
                self.driver = None
            
    def scrape_stock_data(self):
        """Scrape stock data from Dhan website"""
        if not self.driver:
            self.setup_driver()
            
        try:
            logging.info(f"Navigating to {self.url}")
            self.driver.get(self.url)
            
            all_stocks_data = []
            last_height = 0
            scroll_attempts = 0
            max_scroll_attempts = 50  # Increased max attempts
            no_new_data_count = 0
            previous_stock_count = 0
            
            # Initial wait for page load
            time.sleep(10)
            
            while scroll_attempts < max_scroll_attempts:
                logging.info(f"Scroll attempt {scroll_attempts + 1}")
                
                # Scroll in smaller increments
                for i in range(3):  # Scroll 3 times per attempt
                    # Scroll down by viewport height
                    self.driver.execute_script("window.scrollBy(0, window.innerHeight);")
                    time.sleep(8)  # Wait for content to load
                
                # Wait for the stock table to load and find rows
                wait = WebDriverWait(self.driver, 20)
                try:
                    table_rows = wait.until(
                        EC.presence_of_all_elements_located((By.XPATH, "//table//tbody//tr[td]"))
                    )
                except Exception as e:
                    logging.error(f"Error waiting for table rows: {str(e)}")
                    break
                
                if not table_rows:
                    logging.error("Could not find stock table rows")
                    break
                    
                logging.info(f"Found {len(table_rows)} stock rows")
                
                # Process rows
                for row in table_rows:
                    try:
                        # Scroll the row into view to ensure it's loaded
                        self.driver.execute_script("arguments[0].scrollIntoView(true);", row)
                        time.sleep(0.5)  # Small wait for any animations
                        
                        # Get all columns in the row
                        columns = row.find_elements(By.TAG_NAME, "td")
                        
                        if len(columns) >= 5:  # Ensure we have enough columns
                            # Extract data from specific columns
                            name = columns[0].text.strip()  # First column contains name
                            
                            # Skip if empty name or if we already have this stock
                            if not name or any(stock['name'] == name for stock in all_stocks_data):
                                continue
                                
                            price = columns[1].text.strip().replace(',', '')  # Second column is price
                            change_percent = columns[2].text.strip()  # Third column is price change %
                            volume = columns[3].text.strip()  # Fourth column is volume
                            value = columns[4].text.strip()  # Fifth column is value in rupees
                            
                            # Clean up the price (remove ₹ and commas)
                            if price:
                                try:
                                    price = float(price)
                                except ValueError:
                                    logging.warning(f"Could not convert price to float: {price}")
                                    price = 0.0
                            
                            stock_data = {
                                'name': name,
                                'symbol': name.split('\n')[1] if '\n' in name else name,  # Symbol is on second line
                                'price': price,
                                'change_percent': change_percent,
                                'volume': volume,
                                'value': value,
                                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            }
                            
                            all_stocks_data.append(stock_data)
                            logging.debug(f"Extracted stock: {name} - ₹{price}")
                            
                    except Exception as e:
                        logging.warning(f"Error processing row: {str(e)}")
                        continue
                
                # Check if we got new data
                current_stock_count = len(all_stocks_data)
                if current_stock_count == previous_stock_count:
                    no_new_data_count += 1
                    if no_new_data_count >= 5:  # If no new data after 5 attempts, we're probably at the end
                        logging.info("No new data found after multiple attempts, assuming end of list")
                        break
                else:
                    no_new_data_count = 0  # Reset counter if we got new data
                    
                previous_stock_count = current_stock_count
                
                # Get current scroll height
                new_height = self.driver.execute_script("return document.documentElement.scrollHeight")
                
                # Break if no new content was loaded (scroll height didn't change)
                if new_height == last_height:
                    scroll_attempts += 1
                    if scroll_attempts >= 3:  # Try a few more times before giving up
                        # Try one last scroll to the very bottom
                        self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                        time.sleep(3)  # Wait longer for final load
                        
                        # Check height one more time
                        final_height = self.driver.execute_script("return document.documentElement.scrollHeight")
                        if final_height == new_height:
                            logging.info("Reached the end of the list")
                            break
                else:
                    scroll_attempts = 0  # Reset attempts counter if we got new content
                
                # Save screenshot periodically for debugging
                if current_stock_count % 50 == 0:
                    self.driver.save_screenshot(f"scroll_{current_stock_count}_stocks.png")
                
                # Update last height
                last_height = new_height
                
                logging.info(f"Total stocks collected so far: {len(all_stocks_data)}")
                
                # Small wait between scroll attempts
                time.sleep(2)
            
            if all_stocks_data:
                logging.info(f"Successfully extracted {len(all_stocks_data)} stocks")
                return all_stocks_data
            else:
                logging.error("No stock data could be extracted")
                return None
            
        except Exception as e:
            logging.error("Error scraping stock data")
            logging.error(traceback.format_exc())
            return None
            
    def save_to_csv(self, stocks_data):
        """Save stock data to CSV file"""
        try:
            # Define CSV headers
            headers = ['timestamp', 'name', 'symbol', 'price', 'change_percent', 'volume', 'value']
            
            # Check if file exists to determine if we need to write headers
            file_exists = False
            try:
                with open(self.csv_file, 'r') as f:
                    file_exists = True
            except FileNotFoundError:
                pass
            
            # Write data to CSV
            mode = 'a' if file_exists else 'w'
            with open(self.csv_file, mode, newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                
                if not file_exists:
                    writer.writeheader()
                
                writer.writerows(stocks_data)
                
            logging.info(f"Saved {len(stocks_data)} stocks to {self.csv_file}")
            return True
            
        except Exception as e:
            logging.error("Error saving to CSV")
            logging.error(traceback.format_exc())
            return False
            
    def send_csv_email(self):
        """Send the CSV file as an email attachment"""
        try:
            if not os.path.exists(self.csv_file):
                logging.error(f"CSV file {self.csv_file} not found")
                return False

            # Create the email message
            msg = MIMEMultipart()
            msg['From'] = self.email_config['sender_email']
            msg['To'] = self.email_config['recipient_email']
            msg['Subject'] = f'Stock Data Report - {datetime.now().strftime("%Y-%m-%d")}'

            # Add body text
            body = f"Please find attached the stock data report for {datetime.now().strftime('%Y-%m-%d')}."
            msg.attach(MIMEText(body, 'plain'))

            # Attach the CSV file
            with open(self.csv_file, 'rb') as f:
                attachment = MIMEApplication(f.read(), _subtype='csv')
                attachment.add_header('Content-Disposition', 'attachment', filename=self.csv_file)
                msg.attach(attachment)

            # Send the email
            with smtplib.SMTP(self.email_config['smtp_server'], self.email_config['smtp_port']) as server:
                server.starttls()
                server.login(self.email_config['sender_email'], self.email_config['sender_password'])
                server.send_message(msg)

            logging.info(f"CSV file sent successfully to {self.email_config['recipient_email']}")
            return True

        except Exception as e:
            logging.error(f"Error sending email: {str(e)}")
            logging.error(traceback.format_exc())
            return False

    def run_once(self):
        """Run the scraper once and send email"""
        try:
            # Scrape the data
            stocks_data = self.scrape_stock_data()
            if stocks_data:
                # Save to CSV
                if self.save_to_csv(stocks_data):
                    logging.info(f"Successfully updated stock data at {datetime.now()}")
                    # Send email
                    if self.send_csv_email():
                        logging.info("Process completed successfully")
                    else:
                        logging.error("Failed to send email")
                else:
                    logging.error("Failed to save stock data")
            else:
                logging.error("No stock data was scraped")
        except Exception as e:
            logging.error(f"Error in run_once: {str(e)}")
            logging.error(traceback.format_exc())
        finally:
            self.cleanup_driver()

def main():
    scraper = DhanStockScraper()
    
    try:
        scraper.run_once()
    except Exception as e:
        logging.error("Unexpected error in main")
        logging.error(traceback.format_exc())
        scraper.cleanup_driver()

if __name__ == "__main__":
    main() 