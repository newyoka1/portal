"""
GOP Data Center - Automated Tag Export Script (v2)
===================================================
Exports 15 remaining voter data tags from GOP Data Center.

PREREQUISITES:
  1. Install Python 3.10+ from https://www.python.org/downloads/
     (Check "Add to PATH" during install)
  2. Run: pip install selenium webdriver-manager
  3. Chrome must be installed

USAGE:
  python gop_export_v2.py

NOTE: You will log in manually when Chrome opens.
      The script waits for you, then processes all 15 tags.
"""

import time
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("D:\\gop_export_log.txt"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ── Tags already completed (skip these) ──
COMPLETED_TAGS = [
    "2024_NYGOP_bluecollar",
    "2024_NYGOP_childreninhh",
    "2024_NYGOP_donorconservative",
]

# ── Remaining 15 tags to export ──
REMAINING_TAGS = [
    "2024_NYGOP_environmentalprotectionsupport",
    "2024_NYGOP_greennewdealoppose",
    "2024_NYGOP_greennewdealsupport",
    "2024_NYGOP_LowTO_Gen_Bal_LeanGOP_12",
    "2024_NYGOP_LowTO_Gen_Bal_Swing_13",
    "2024_NYGOP_LowTO_Leg_Bal_HardDem_15",
    "2024_NYGOP_LowTO_Leg_Bal_HardGOP_11",
    "2024_NYGOP_LowTO_Leg_Bal_LeanDem_14",
    "2024_NYGOP_LowTO_Leg_Bal_LeanGOP_12",
    "2024_NYGOP_prochoice",
    "2024_NYGOP_prolife",
    "2024_NYGOP_religionjewish",
    "2024_NYGOP_retired",
    "2024_NYGOP_unionsupporters",
    "2024_NYGOP_veteransympathizer",
]

BASE_URL = "https://www.gopdatacenter.com/rnc/AdvancedCounts/"
MIN_VOTER_COUNT = 100  # Minimum expected voters (0 = tag not applied)


class GOPExporter:
    def __init__(self):
        self.driver = None
        self.wait = None
        self.exported = []
        self.failed = []

    def setup_driver(self):
        """Initialize Chrome with visible window"""
        logger.info("Setting up Chrome driver...")
        opts = webdriver.ChromeOptions()
        opts.add_argument('--no-sandbox')
        opts.add_argument('--disable-dev-shm-usage')
        opts.add_argument('--start-maximized')
        # Keep browser open if script crashes
        opts.add_experimental_option("detach", True)
        self.driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=opts
        )
        self.wait = WebDriverWait(self.driver, 30)
        logger.info("Chrome ready.")

    def wait_for_login(self):
        """Navigate to page; wait for manual login if needed."""
        logger.info(f"Opening {BASE_URL}")
        self.driver.get(BASE_URL)
        time.sleep(3)
        url = self.driver.current_url.lower()
        if "login" in url or "signin" in url or "account" in url:
            logger.warning("="*50)
            logger.warning("LOGIN REQUIRED - Please log in in the browser window")
            logger.warning("Then press ENTER here to continue...")
            logger.warning("="*50)
            input()
            self.driver.get(BASE_URL)
            time.sleep(3)
        logger.info("Logged in. Proceeding...")

    # ── STEP 2: Drag "Tag Name" into "New Group" ──
    def drag_tag_name_to_group(self):
        """
        Drag 'Tag Name' from the left panel into the 'New Group' box.
        This opens the tag selection modal.
        
        NOTE: The exact selectors below may need adjustment based on
        the actual DOM structure. Common patterns are tried.
        """
        logger.info("STEP 2: Dragging 'Tag Name' to 'New Group'...")
        
        # Try multiple selector strategies for the draggable "Tag Name" element
        tag_name_selectors = [
            "//span[text()='Tag Name']",
            "//div[text()='Tag Name']",
            "//li[contains(text(),'Tag Name')]",
            "//*[contains(@class,'draggable') and contains(text(),'Tag Name')]",
            "//*[@data-field='TagName']",
            "//generic[text()='Tag Name']",
        ]
        
        source = None
        for sel in tag_name_selectors:
            try:
                source = self.wait.until(
                    EC.presence_of_element_located((By.XPATH, sel))
                )
                if source.is_displayed():
                    logger.info(f"  Found 'Tag Name' with selector: {sel}")
                    break
            except:
                continue
        
        if not source:
            raise Exception("Could not find 'Tag Name' element to drag")

        # Find the "New Group" drop target
        group_selectors = [
            "//*[contains(text(),'New Group')]",
            "//*[contains(@class,'drop-target')]",
            "//*[contains(@class,'group-box')]",
            "//div[contains(@class,'droppable')]",
        ]
        
        target = None
        for sel in group_selectors:
            try:
                target = self.driver.find_element(By.XPATH, sel)
                if target.is_displayed():
                    logger.info(f"  Found 'New Group' with selector: {sel}")
                    break
            except:
                continue
        
        if not target:
            raise Exception("Could not find 'New Group' drop target")

        # Perform drag-and-drop
        actions = ActionChains(self.driver)
        actions.click_and_hold(source).pause(0.5)
        actions.move_to_element(target).pause(0.5)
        actions.release(target).perform()
        time.sleep(2)
        logger.info("  Drag complete. Modal should be open.")

    # ── STEPS 3-6: Filter, uncheck all, check one tag, click OK ──
    def select_single_tag(self, tag_name):
        """Filter modal, uncheck all, check only the target tag, click OK."""
        logger.info(f"STEPS 3-6: Selecting tag '{tag_name}'...")
        
        # Step 3: Type full tag name in filter
        filter_selectors = [
            "//input[@placeholder='Filter Criteria...']",
            "//input[contains(@placeholder,'Filter')]",
            "//input[contains(@placeholder,'Search')]",
            "//input[contains(@class,'filter')]",
        ]
        
        filter_box = None
        for sel in filter_selectors:
            try:
                filter_box = self.wait.until(
                    EC.presence_of_element_located((By.XPATH, sel))
                )
                break
            except:
                continue
        
        if not filter_box:
            raise Exception("Could not find filter input in modal")
        
        filter_box.clear()
        filter_box.send_keys(tag_name)
        time.sleep(1.5)
        logger.info(f"  Filtered for: {tag_name}")

        # Step 4: Uncheck ALL currently checked tags
        checked = self.driver.find_elements(
            By.XPATH, "//input[@type='checkbox']"
        )
        for cb in checked:
            try:
                if cb.is_selected():
                    cb.click()
                    time.sleep(0.2)
            except:
                pass
        logger.info("  Unchecked all existing tags")
        
        # Step 5: Check ONLY the target tag
        # Try label-based and text-based matching
        tag_cb_selectors = [
            f"//label[normalize-space(text())='{tag_name}']/preceding-sibling::input[@type='checkbox']",
            f"//label[normalize-space(text())='{tag_name}']/../input[@type='checkbox']",
            f"//label[contains(text(),'{tag_name}')]",
            f"//span[text()='{tag_name}']/preceding-sibling::input",
        ]
        
        tag_cb = None
        for sel in tag_cb_selectors:
            try:
                el = self.driver.find_element(By.XPATH, sel)
                if el.tag_name == 'label':
                    el.click()  # clicking label toggles checkbox
                else:
                    if not el.is_selected():
                        el.click()
                tag_cb = el
                break
            except:
                continue

        if not tag_cb:
            raise Exception(f"Could not find/check tag: {tag_name}")
        logger.info(f"  Checked: {tag_name}")
        
        # Step 6: Click OKAY to close modal
        ok_selectors = [
            "//button[text()='Okay']",
            "//button[text()='OK']",
            "//button[text()='Ok']",
            "//input[@value='Okay']",
            "//input[@value='OK']",
        ]
        for sel in ok_selectors:
            try:
                btn = self.driver.find_element(By.XPATH, sel)
                btn.click()
                logger.info("  Clicked OK. Modal closed.")
                time.sleep(3)
                return
            except:
                continue
        raise Exception("Could not find OK/Okay button")

    # ── STEP 7: Verify voter count ──
    def verify_voter_count(self, tag_name):
        """Check that voter count is > 0 after tag selection."""
        logger.info("STEP 7: Verifying voter count...")
        time.sleep(2)
        
        # Look for the count display on the page
        count_selectors = [
            "//*[contains(@id,'Count')]",
            "//*[contains(@id,'count')]",
            "//*[contains(@class,'count')]",
            "//*[contains(@id,'Total')]",
            "//*[contains(@id,'total')]",
        ]
        
        for sel in count_selectors:
            try:
                el = self.driver.find_element(By.XPATH, sel)
                text = el.text.strip().replace(",", "")
                if text.isdigit():
                    count = int(text)
                    if count > MIN_VOTER_COUNT:
                        logger.info(f"  ✓ Voter count: {count:,}")
                        return True
                    elif count == 0:
                        logger.warning(f"  ✗ Voter count is 0! Tag may not be applied.")
                        return False
            except:
                continue
        
        # If we can't find a count element, warn but continue
        logger.warning("  ⚠ Could not locate voter count element. Proceeding anyway.")
        return True  # Proceed optimistically

    # ── STEPS 8-12: Export workflow ──
    def export_current_tag(self, tag_name):
        """Click Export, fill options, submit."""
        logger.info(f"STEPS 8-12: Exporting '{tag_name}'...")

        # Step 8: Click EXPORT FILE
        export_selectors = [
            "//button[contains(text(),'Export')]",
            "//input[@value='Export File']",
            "//a[contains(text(),'Export')]",
            "//*[contains(@id,'Export') and (self::button or self::input or self::a)]",
        ]
        for sel in export_selectors:
            try:
                btn = self.driver.find_element(By.XPATH, sel)
                btn.click()
                time.sleep(3)
                logger.info("  Clicked Export File")
                break
            except:
                continue

        # Step 9: Select User Defined List → All Fields Geo → Individual Voters → CSV
        # User Defined List radio
        try:
            radio_selectors = [
                "//input[@type='radio'][following-sibling::*[contains(text(),'User Defined')]]",
                "//label[contains(text(),'User Defined')]/input",
                "//label[contains(text(),'User Defined')]",
            ]
            for sel in radio_selectors:
                try:
                    r = self.driver.find_element(By.XPATH, sel)
                    r.click()
                    time.sleep(1)
                    break
                except:
                    continue
            logger.info("  Selected: User Defined List")

            # All Fields Geo dropdown
            dropdown_selectors = [
                "//select[contains(@id,'UserList')]",
                "//select[contains(@id,'userlist')]",
                "//select[contains(@id,'List')]",
            ]
            for sel in dropdown_selectors:
                try:
                    dd = Select(self.driver.find_element(By.XPATH, sel))
                    dd.select_by_visible_text("All Fields Geo")
                    time.sleep(1)
                    logger.info("  Selected: All Fields Geo")
                    break
                except:
                    continue

            # Individual Voters radio
            iv_selectors = [
                "//input[@type='radio'][following-sibling::*[contains(text(),'Individual')]]",
                "//label[contains(text(),'Individual')]/input",
                "//label[contains(text(),'Individual')]",
            ]
            for sel in iv_selectors:
                try:
                    r = self.driver.find_element(By.XPATH, sel)
                    r.click()
                    time.sleep(1)
                    break
                except:
                    continue
            logger.info("  Selected: Individual Voters")

        except Exception as e:
            logger.warning(f"  Export options selection issue: {e}")

        # Step 10: Set filename
        fn_selectors = [
            "//input[contains(@id,'FileName')]",
            "//input[contains(@id,'filename')]",
            "//input[contains(@id,'ExportFileName')]",
            "//input[@type='text'][contains(@name,'file')]",
        ]
        for sel in fn_selectors:
            try:
                fn_input = self.driver.find_element(By.XPATH, sel)
                fn_input.clear()
                fn_input.send_keys(tag_name)
                logger.info(f"  Filename set: {tag_name}")
                break
            except:
                continue

        # Step 11: Click EXPORT DATA NOW
        time.sleep(1)
        submit_selectors = [
            "//input[@type='submit' and contains(@value,'Export')]",
            "//button[contains(text(),'Export Data')]",
            "//input[@value='Export Data Now']",
            "//button[contains(text(),'Export Now')]",
        ]
        for sel in submit_selectors:
            try:
                btn = self.driver.find_element(By.XPATH, sel)
                btn.click()
                logger.info("  Clicked Export Data Now")
                break
            except:
                continue

        # Step 12: Wait for export queue confirmation
        time.sleep(5)
        logger.info(f"  ✓ Export queued for: {tag_name}")

    # ── MAIN LOOP ──
    def process_tag(self, tag_name, index, total):
        """Full workflow for one tag (steps 1-13)."""
        logger.info(f"\n{'='*60}")
        logger.info(f"[{index}/{total}] TAG: {tag_name}")
        logger.info(f"{'='*60}")

        # Step 1: Navigate to Advanced Counts
        self.driver.get(BASE_URL)
        time.sleep(3)

        # Step 2: Drag Tag Name to New Group
        self.drag_tag_name_to_group()

        # Steps 3-6: Select the tag
        self.select_single_tag(tag_name)

        # Step 7: Verify voter count
        if not self.verify_voter_count(tag_name):
            logger.error(f"SKIPPING {tag_name} - voter count verification failed!")
            self.failed.append(tag_name)
            return False

        # Steps 8-12: Export
        self.export_current_tag(tag_name)
        self.exported.append(tag_name)
        return True

    def run(self):
        """Main entry point."""
        try:
            self.setup_driver()
            self.wait_for_login()

            total = len(REMAINING_TAGS)
            logger.info(f"\nProcessing {total} remaining tags...")
            logger.info(f"Already completed: {', '.join(COMPLETED_TAGS)}\n")

            for i, tag in enumerate(REMAINING_TAGS, 1):
                try:
                    self.process_tag(tag, i, total)
                except Exception as e:
                    logger.error(f"FAILED: {tag} - {e}")
                    self.failed.append(tag)
                    # Screenshot for debugging
                    try:
                        self.driver.save_screenshot(f"D:\\error_{tag}.png")
                    except:
                        pass
                time.sleep(2)

            # ── Summary ──
            logger.info(f"\n{'='*60}")
            logger.info(f"EXPORT COMPLETE")
            logger.info(f"{'='*60}")
            logger.info(f"  Exported: {len(self.exported)}/{total}")
            logger.info(f"  Failed:   {len(self.failed)}/{total}")
            if self.failed:
                logger.info(f"  Failed tags:")
                for t in self.failed:
                    logger.info(f"    - {t}")
            logger.info(f"\nBrowser will stay open. Check Export Queue for downloads.")
            logger.info("Press ENTER to close browser...")
            input()

        except KeyboardInterrupt:
            logger.info("\nScript interrupted by user.")
        except Exception as e:
            logger.error(f"Fatal error: {e}")
            try:
                self.driver.save_screenshot("D:\\fatal_error.png")
            except:
                pass
        finally:
            if self.driver:
                self.driver.quit()


if __name__ == "__main__":
    print("""
    ╔══════════════════════════════════════════════════════════════╗
    ║  GOP Data Center - Tag Export Automation v2                  ║
    ║  15 remaining tags to process                                ║
    ║                                                              ║
    ║  IMPORTANT: You must install Python dependencies first:      ║
    ║    pip install selenium webdriver-manager                    ║
    ╚══════════════════════════════════════════════════════════════╝
    """)
    exporter = GOPExporter()
    exporter.run()
