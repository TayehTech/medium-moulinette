import json
import os
import asyncio
import aiohttp
import aiofile

PDF_URLS_FILE = "./pdf_urls.json"
PDF_FOLDER = "pdf"

def get_file_path(project_name):
    return os.path.join(PDF_FOLDER, project_name + ".pdf")

def download_pdf_files():
    projects_json = {}
    with open(PDF_URLS_FILE) as f:
        projects_json = json.load(f)
    
    if projects_json == {}:
        return []
    
    os.makedirs(PDF_FOLDER, exist_ok=True)
    sema = asyncio.BoundedSemaphore(5)

    async def fetch_file(session, project_name, url):
        out_path = get_file_path(project_name)

        if os.path.exists(out_path):
            return
        
        async with sema:
            async with session.get(url) as resp:
                assert resp.status == 200
                data = await resp.read()

        async with aiofile.async_open(out_path, "wb") as outfile:
            await outfile.write(data)

    async def main_loop():
        async with aiohttp.ClientSession() as session:
            tasks = [fetch_file(session, project["name"], project["url"]) 
                        for project in projects_json["projects"]]
            await asyncio.gather(*tasks)
        

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main_loop())
    loop.close()

    return [get_file_path(project["name"]) for project in projects_json["projects"]]

if __name__ == "__main__":
    download_pdf_files()