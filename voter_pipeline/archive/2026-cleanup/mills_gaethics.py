import asyncio
from playwright.async_api import async_playwright

async def search_ga_ethics():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://media.ethics.ga.gov/search/Campaign/Campaign_ByName.aspx", timeout=30000)
        # Fill the search form
        await page.fill("input[name*='LastName'], input[id*='LastName']", "Mills")
        await page.fill("input[name*='FirstName'], input[id*='FirstName']", "Michael")
        # Click search button
        await page.click("input[type='submit'], input[value*='Search'], button[type='submit']")
        await page.wait_for_load_state("networkidle", timeout=15000)
        content = await page.content()
        await browser.close()
        return content

async def main():
    result = await search_ga_ethics()
    # Print relevant portion
    import re
    # Strip HTML tags for readability
    text = re.sub(r'<[^>]+>', ' ', result)
    text = re.sub(r'\s+', ' ', text)
    print(text[2000:6000])

asyncio.run(main())
