import asyncio
from playwright.async_api import async_playwright

async def check_nysed():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://eservices.nysed.gov/professions/verification-search", timeout=30000)
        await page.wait_for_load_state("networkidle", timeout=20000)
        
        # Select profession: Mental Health Counselor (ID 45)
        await page.select_option("select[name='professionId']", "45")
        await page.wait_for_timeout(1000)
        
        # Fill last name
        await page.fill("input[name='lastName']", "Mills")
        await page.fill("input[name='firstName']", "Michael")
        
        # Submit
        await page.click("input[type='submit'], button[type='submit']")
        await page.wait_for_load_state("networkidle", timeout=15000)
        
        content = await page.content()
        text = await page.inner_text("body")
        print(text[:3000])
        await browser.close()

asyncio.run(check_nysed())
