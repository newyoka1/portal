import asyncio
from playwright.async_api import async_playwright

async def inspect_nysed():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://eservices.nysed.gov/professions/verification-search", timeout=30000)
        # Wait for JS to render
        await page.wait_for_timeout(8000)
        
        selects = await page.query_selector_all("select")
        print(f"Selects found: {len(selects)}")
        for s in selects:
            name = await s.get_attribute("name")
            id_ = await s.get_attribute("id")
            cls = await s.get_attribute("class")
            print(f"  select name={name} id={id_} class={cls}")
        
        inputs = await page.query_selector_all("input")
        print(f"Inputs found: {len(inputs)}")
        for i in inputs:
            name = await i.get_attribute("name")
            type_ = await i.get_attribute("type")
            id_ = await i.get_attribute("id")
            placeholder = await i.get_attribute("placeholder")
            print(f"  input type={type_} name={name} id={id_} placeholder={placeholder}")
        
        # Also print page title and any headings
        title = await page.title()
        print(f"\nPage title: {title}")
        body_text = await page.inner_text("body")
        print(body_text[:1000])
        
        await browser.close()

asyncio.run(inspect_nysed())
