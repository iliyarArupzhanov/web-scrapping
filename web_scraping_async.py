# importing essential libraries
import aiohttp
import asyncio
import re

from asyncio.windows_events import DefaultEventLoopPolicy, WindowsSelectorEventLoopPolicy
from random import choice, uniform
from bs4 import BeautifulSoup
import time
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] - %(message)s')
logger = logging.getLogger(__name__)


# dataframe to store information
flats_df = pd.DataFrame()


# function to check whether tag is None or not
def get_info(tag):
    if tag is None:
        return None
    else:
        return tag.find("div", class_="offer__advert-short-info").text


# function to process each flat
async def process_card(card_url, session, user_agent, semaphore):
    headers = {"User-Agent": user_agent}
    await asyncio.sleep(uniform(1, 3))

    async with semaphore, session.get(url=card_url, headers=headers) as response:
        await asyncio.sleep(uniform(1, 3))
        response_text = await response.text()

        soup_of_card_url = BeautifulSoup(response_text, 'lxml')
        data = soup_of_card_url.find("div", class_="offer__container")

        try:
            date_ad = data.find("div", class_="offer__views").text.split("{nb}")[-1].strip()
        except AttributeError:
            date_ad = None

        try:
            city = data.find("div", class_="offer__location offer__advert-short-info").text.strip().split("\nпоказать")[
                0]
        except AttributeError:
            city = None

        res_complex = get_info(data.find("div", {'data-name': 'map.complex'}))
        building_type = get_info(data.find("div", {'data-name': 'flat.building'}))
        year = get_info(data.find("div", {'data-name': 'house.year'}))
        floor = get_info(data.find("div", {'data-name': 'flat.floor'}))
        square = get_info(data.find("div", {'data-name': 'live.square'}))
        renovation = get_info(data.find("div", {'data-name': 'flat.renovation'}))

        try:
            description = data.find("div", class_="text").text.replace("\n", " ")
        except AttributeError:
            description = None

    logger.info(f"Processing card {card_url} is done!")
    return {
        "city": city,
        "res_complex": res_complex,
        "building_type": building_type,
        "year": year,
        "floor": floor,
        "square": square,
        "renovation": renovation,
        "description": description,
        "date_ad": date_ad
    }


# function to process each page with flats
async def process_page(page_url, session, user_agent, semaphore):
    headers = {"User-Agent": user_agent}
    await asyncio.sleep(uniform(1, 3))

    async with semaphore, session.get(url=page_url, headers=headers) as response:
        await asyncio.sleep(uniform(1, 3))
        response_text = await response.text()

        soup = BeautifulSoup(response_text, 'lxml')
        pattern = re.compile(r'.*a-card a-storage-live ddl_product ddl_product_link.*')
        flats = soup.find_all("div", class_=pattern)

        tasks = []

        for flat in flats:
            # get the short description of flat
            card_url = "https://krisha.kz" + flat.find("a").get("href")
            img_url = flat.find("img").get("src")

            try:
                short_descr = flat.find("a", class_="a-card__title").text.strip()
            except AttributeError:
                short_descr = None

            try:
                price = flat.find("div", class_="a-card__price").text.strip()
            except AttributeError:
                price = None

            try:
                address = flat.find("div", class_="a-card__subtitle").text.strip()
            except AttributeError:
                address = None

            task = asyncio.create_task(process_card(card_url, session, user_agent, semaphore))
            tasks.append(task)

    results = await asyncio.gather(*tasks)

    logger.info(f"Processing page {page_url} is done!")
    # Return a list of dictionaries containing scraped data for each flat on the page
    return [{"card_url": card_url,
             "img_url": img_url,
             "short_descr": short_descr,
             "price": price,
             "address": address,
             **result} for result in results]


async def main():
    # randomly choose the user_agent to avoid being identified as a bot
    user_agents = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                   "AppleWebKit/537.36 (KHTML, like Gecko)", "Chrome/110.0.0.0", "Safari/537.36", "OPR/96.0.0.0"]
    user_agent = choice(user_agents)
    headers = {"User-Agent": user_agent}
    url = "https://krisha.kz/prodazha/kvartiry/"

    # limit the number of concurrent requests
    concurrent_requests_limit = 5
    semaphore = asyncio.Semaphore(concurrent_requests_limit)

    async with aiohttp.ClientSession() as session:
        response = await session.get(url=url, headers=headers)
        soup = BeautifulSoup(await response.text(), 'lxml')
        pages_count = int(soup.find("nav", class_="paginator").find_all("a")[-2].text.strip())

        tasks = []
        for page in range(1, 40):
            url = f"https://krisha.kz/prodazha/kvartiry/?page={page}"
            task = asyncio.create_task(process_page(url, session, user_agent, semaphore))
            tasks.append(task)

        results = await asyncio.gather(*tasks)

        flats_data = []
        for page_result in results:
            for flat_data in page_result:
                flats_data.append(flat_data)

        global flats_df
        flats_df = pd.DataFrame(flats_data)


if __name__ == '__main__':
    start_time = time.time()

    # limit the number of concurrent requests
    concurrent_requests_limit = 5
    semaphore = asyncio.Semaphore(concurrent_requests_limit)

    asyncio.set_event_loop_policy(DefaultEventLoopPolicy())
    asyncio.run(main())

    # create excel file with obtained data
    flats_df.columns = ['Url объявления', 'Url фото', 'Описание', 'Цена', 'Адрес', 'Город', 'Жилой комплекс',
                        'Тип дома', 'Год постройки', 'Этаж', 'Площадь', 'Состояние', 'Полное описание',
                        'Дата объявления']
    flats_df.to_excel("Данные с Krisha.xlsx", index=False)
    end_time = time.time()
    print(end_time - start_time)
