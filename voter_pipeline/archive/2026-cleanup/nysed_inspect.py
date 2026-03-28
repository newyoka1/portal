import asyncio
from playwright.async_api import async_playwright

async def inspect_nysed():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://eservices.nysed.gov/professions/verification-search", timeout=30000)
        await page.wait_for_load_state("domcontentloaded", timeout=20000)
        await page.wait_for_timeout(3000)
        
        # Get all form inputs and selects
        html = await page.content()
        # Print key form elements
        selects = await page.query_selector_all("select")
        print(f"Selects found: {len(selects)}")
        for s in selects:
            name = await s.get_attribute("name")
            id_ = await s.get_attribute("id")
            print(f"  select name={name} id={id_}")
        
        inputs = await page.query_selector_all("input")
        print(f"\nInputs found: {len(inputs)}")
        for i in inputs:
            name = await i.get_attribute("name")
            type_ = await i.get_attribute("type")
            id_ = await i.get_attribute("id")
            print(f"  input type={type_} name={name} id={id_}")
        
        await browser.close()

asyncio.run(inspect_nysed())
