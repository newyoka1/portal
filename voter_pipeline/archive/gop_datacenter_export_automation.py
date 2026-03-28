"""
GOP Data Center - Automated Tag Export Script
Exports all tags starting with "2024_NYGOP" from the GOP Data Center

Requirements:
- selenium
- webdriver-manager

Install with: pip install selenium webdriver-manager
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GOPDataCenterExporter:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.base_url = "https://www.gopdatacenter.com/rnc/AdvancedCounts/"
        
    def setup_driver(self):
        """Initialize Chrome driver"""
        logger.info("Setting up Chrome driver...")
        options = webdriver.ChromeOptions()
        # options.add_argument('--headless')  # Uncomment to run headless
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--start-maximized')
        
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=options
        )
        self.wait = WebDriverWait(self.driver, 20)
        logger.info("Chrome driver setup complete")
        
    def login_check(self):
        """
        Navigate to the page and wait for user to login if needed
        """
        logger.info(f"Navigating to {self.base_url}")
        self.driver.get(self.base_url)
        
        # Check if we need to login
        time.sleep(3)
        current_url = self.driver.current_url
        
        if "login" in current_url.lower() or "signin" in current_url.lower():
            logger.warning("Login required! Please login in the browser window...")
            logger.warning("Press ENTER here after you've logged in...")
            input()
            logger.info("Continuing after login...")
        else:
            logger.info("Already logged in or no login required")
            
    def get_all_nygop_tags(self):
        """
        Get all tag names starting with 2024_NYGOP
        Returns list of tag names
        """
        logger.info("Opening Tag Name selection modal...")
        
        # Expand TAGS section if needed
        try:
            tags_button = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "//button[contains(., 'TAGS')]"))
            )
            # Check if already expanded
            parent = tags_button.find_element(By.XPATH, "./..")
            if "collapsed" in parent.get_attribute("class"):
                tags_button.click()
                time.sleep(1)
        except Exception as e:
            logger.warning(f"Could not expand TAGS section: {e}")
        
        # Click "Tag Name" to open modal
        tag_name_link = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//generic[text()='Tag Name']"))
        )
        tag_name_link.click()
        time.sleep(2)
        
        # Click Edit button (the one next to the current tag)
        try:
            edit_button = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "//label[contains(text(), 'Edit the criteria')]"))
            )
            edit_button.click()
            time.sleep(2)
        except:
            logger.info("No existing tag to edit, modal should already be open")
        
        # Find and click the filter box
        filter_box = self.wait.until(
            EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Filter Criteria...']"))
        )
        filter_box.clear()
        filter_box.send_keys("2024_nygop")
        time.sleep(2)
        
        # Get all matching tag checkboxes
        tag_elements = self.driver.find_elements(
            By.XPATH, 
            "//label[starts-with(text(), '2024_NYGOP')]"
        )
        
        tags = []
        for elem in tag_elements:
            tag_name = elem.text.strip()
            if tag_name and tag_name.startswith("2024_NYGOP"):
                tags.append(tag_name)
        
        logger.info(f"Found {len(tags)} tags starting with 2024_NYGOP:")
        for tag in tags:
            logger.info(f"  - {tag}")
        
        # Close the modal by clicking Cancel
        cancel_button = self.driver.find_element(By.XPATH, "//button[text()='Cancel']")
        cancel_button.click()
        time.sleep(1)
        
        return sorted(list(set(tags)))  # Remove duplicates and sort
    
    def select_tag(self, tag_name):
        """
        Select a specific tag
        """
        logger.info(f"Selecting tag: {tag_name}")
        
        # Click Edit button to open modal
        edit_button = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//label[contains(text(), 'Edit the criteria')]"))
        )
        edit_button.click()
        time.sleep(2)
        
        # Filter for the tag
        filter_box = self.wait.until(
            EC.presence_of_element_located((By.XPATH, "//input[@placeholder='Filter Criteria...']"))
        )
        filter_box.clear()
        filter_box.send_keys("2024_nygop")
        time.sleep(1)
        
        # Uncheck all checkboxes first
        checked_boxes = self.driver.find_elements(
            By.XPATH,
            "//input[@type='checkbox' and contains(@id, 'Tag')][@checked]"
        )
        for box in checked_boxes:
            if box.is_selected():
                box.click()
                time.sleep(0.3)
        
        # Find and check the specific tag
        tag_checkbox = self.wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, f"//label[text()='{tag_name}']/preceding-sibling::input[@type='checkbox']")
            )
        )
        tag_checkbox.click()
        time.sleep(1)
        
        # Click OKAY
        okay_button = self.driver.find_element(By.XPATH, "//button[text()='Okay']")
        okay_button.click()
        time.sleep(3)
        
        logger.info(f"Tag {tag_name} selected successfully")
    
    def export_tag(self, tag_name):
        """
        Export the currently selected tag
        """
        logger.info(f"Exporting tag: {tag_name}")
        
        # Click EXPORT FILE button
        export_button = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[text()='Export File']"))
        )
        export_button.click()
        time.sleep(3)
        
        # Wait for export page to load
        self.wait.until(EC.url_contains("exportcounts.aspx"))
        
        # Select "User Defined List" radio button
        user_defined_radio = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//input[@type='radio' and following-sibling::text()[contains(., 'User Defined List')]]"))
        )
        user_defined_radio.click()
        time.sleep(2)
        
        # Select "All Fields Geo" from dropdown
        select_dropdown = Select(
            self.driver.find_element(By.XPATH, "//select[contains(@id, 'UserList')]")
        )
        select_dropdown.select_by_visible_text("All Fields Geo")
        time.sleep(1)
        
        # Select "Individual Voters" radio button
        individual_voters_radio = self.wait.until(
            EC.element_to_be_clickable(
                (By.XPATH, "//input[@type='radio' and following-sibling::text()[contains(., 'Individual Voters')]]")
            )
        )
        individual_voters_radio.click()
        time.sleep(1)
        
        # Set export filename
        filename_input = self.driver.find_element(By.XPATH, "//input[@type='text' and contains(@id, 'ExportFileName')]")
        filename_input.clear()
        filename_input.send_keys(tag_name)
        time.sleep(1)
        
        # Click EXPORT DATA NOW
        export_now_button = self.driver.find_element(
            By.XPATH, "//input[@type='submit' and @value='Export Data Now']"
        )
        export_now_button.click()
        time.sleep(3)
        
        # Wait for export queue page
        self.wait.until(EC.url_contains("ExportQueue.aspx"))
        logger.info(f"Export queued for {tag_name}")
        
        # Navigate back to Advanced Counts
        self.driver.get(self.base_url)
        time.sleep(3)
    
    def run(self):
        """
        Main execution flow
        """
        try:
            self.setup_driver()
            self.login_check()
            
            # Get all tags
            tags = self.get_all_nygop_tags()
            
            if not tags:
                logger.error("No tags found starting with 2024_NYGOP!")
                return
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Starting export process for {len(tags)} tags")
            logger.info(f"{'='*60}\n")
            
            # Export each tag
            for i, tag in enumerate(tags, 1):
                logger.info(f"\n[{i}/{len(tags)}] Processing: {tag}")
                try:
                    self.select_tag(tag)
                    self.export_tag(tag)
                    logger.info(f"✓ Successfully queued export for {tag}")
                except Exception as e:
                    logger.error(f"✗ Failed to export {tag}: {e}")
                    # Take screenshot for debugging
                    screenshot_path = f"D:\\error_{tag}.png"
                    self.driver.save_screenshot(screenshot_path)
                    logger.error(f"Screenshot saved to {screenshot_path}")
                    continue
                
                # Small delay between exports
                time.sleep(2)
            
            logger.info(f"\n{'='*60}")
            logger.info(f"Export process complete!")
            logger.info(f"All {len(tags)} tags have been queued for export")
            logger.info(f"Files will be available in the Export Queue when processing completes")
            logger.info(f"{'='*60}\n")
            
            # Navigate to export queue to show status
            self.driver.get("https://www.gopdatacenter.com/rnc/counts/ExportQueue.aspx")
            logger.info("Showing export queue. Browser will remain open for you to download files.")
            logger.info("Press ENTER to close the browser...")
            input()
            
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            # Save screenshot
            if self.driver:
                self.driver.save_screenshot("D:\\fatal_error.png")
        finally:
            if self.driver:
                self.driver.quit()
                logger.info("Browser closed")


if __name__ == "__main__":
    print("""
    ╔════════════════════════════════════════════════════════════╗
    ║   GOP Data Center - Automated Tag Export Script           ║
    ║   Exports all tags starting with "2024_NYGOP"             ║
    ╚════════════════════════════════════════════════════════════╝
    """)
    
    exporter = GOPDataCenterExporter()
    exporter.run()
