import aiofiles
import httpx
import json
from selectolax.parser import HTMLParser
from pydantic import BaseModel
import asyncio

class GoogleResult(BaseModel):
    domain_url: str
    snippets: list

async def scrape_domains(input_file):
    async with aiofiles.open(input_file, 'r', encoding="utf-8") as f:
        lines = await f.readlines()
        domain = [line[line.find('@') + 1:].strip() for line in lines if '@' in line]
    return domain

async def get_html(domain_url):
    query = f"CEO of {domain_url}"
    url = f"https://www.google.com/search?q={query}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"
    }
    async with httpx.AsyncClient() as client:
        response = await client.get(url, headers=headers)
    return HTMLParser(response.text)

def parse_result(html):
    page_result = html.css("div.MjjYud")
    snippets = []
    for i in page_result:
        search_snippets = i.css_first(".VwiC3b.yXK7lf.lVm3ye.r025kc.hJNv6b.Hdw6tb")
        snippet = search_snippets.css_first("span").text() if search_snippets and search_snippets.css_first("span") else None

        if snippet:  # Only add snippets that are not N/A
            snippets.append(snippet)
        
    return snippets

async def main(input_file):
    domains = await scrape_domains(input_file)

    all_results = []
    for domain in domains:
        search_result = await get_html(domain)
        results = parse_result(search_result)
        if results:  # Only add to results if there are valid snippets
            google_result = GoogleResult(domain_url=domain, snippets=results)
            all_results.append(google_result)

    with open('google_results.json', 'a', encoding='utf-8') as f:
        json.dump([r.dict() for r in all_results], f, ensure_ascii=False, indent=4)

if __name__ == "__main__":
    input_file = 'input.txt'  # Specify your input file here
    asyncio.run(main(input_file))

