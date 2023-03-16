import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
from random import choice


# function to get "soup"
def get_soup(url, user_agent):
    # add delay to not overload the server
    time.sleep(3)
    headers = {"User-Agent": user_agent}
    time.sleep(3)
    response = requests.get(url, headers=headers)
    time.sleep(3)
    soup = BeautifulSoup(response.text, "lxml")

    return soup

# check whether tag is None or not
def get_info(tag):
    if tag is None:
        return None
    else:
        return tag.find("div", class_="offer__advert-short-info").text


def main(url, df, user_agent):
    soup = get_soup(url, user_agent)

    flats = soup.find_all("div", class_="a-card a-storage-live ddl_product ddl_product_link not-colored is-visible")

    # check if page is empty or not
    if len(flats) == 0:
        return True

    for flat in flats:
        # get the short description of flat
        card_url = "https://krisha.kz" + flat.find("a").get("href")

        img_url = flat.find("img").get("src")

        try:
            short_descr = flat.find("a", class_="a-card__title").text.strip()
        except BaseException:
            short_descr = None

        try:
            price = flat.find("div", class_="a-card__price").text.strip()
        except BaseException:
            price = None

        try:
            address = flat.find("div", class_="a-card__subtitle").text.strip()
        except BaseException:
            address = None

        # get the detailed information by following the url of specific flat
        soup_of_card_url = get_soup(card_url, user_agent)

        data = soup_of_card_url.find("div", class_="offer__container")

        try:
            date_ad = data.find("div", class_="offer__views").text.split("{nb}")[-1].strip()
        except BaseException:
            date_ad = None

        try:
            city = data.find("div", class_="offer__location offer__advert-short-info").text.strip().split("\nпоказать")[
                0]
        except BaseException:
            city = None

        res_complex = get_info(data.find("div", {'data-name': 'map.complex'}))
        building_type = get_info(data.find("div", {'data-name': 'flat.building'}))
        year = get_info(data.find("div", {'data-name': 'house.year'}))
        floor = get_info(data.find("div", {'data-name': 'flat.floor'}))
        square = get_info(data.find("div", {'data-name': 'live.square'}))
        renovation = get_info(data.find("div", {'data-name': 'flat.renovation'}))

        try:
            description = data.find("div", class_="text").text.replace("\n", " ")
        except BaseException:
            description = None

        # add scraped values into the empty dataframe
        df.loc[len(df)] = [card_url, img_url, date_ad, short_descr, city, address, price, res_complex, building_type,
                           year, floor, square, renovation, description]


if __name__ == "__main__":

    # randomly choose the user_agent to avoid being identified as a bot
    user_agents = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
                   "AppleWebKit/537.36 (KHTML, like Gecko)", "Chrome/110.0.0.0", "Safari/537.36", "OPR/96.0.0.0"]
    user_agent = choice(user_agents)

    columns = ['Url объявления', 'Url фото', 'Дата объявления', 'Описание', 'Город', 'Адрес', 'Цена', 'Жилой комплекс',
               'Тип дома', 'Год постройки', 'Этаж', 'Площадь', 'Состояние', 'Полное описание']
    df = pd.DataFrame(columns=columns)

    page_number = 1
    #while True
    for page_number in range(1, 40):

        url = f"https://krisha.kz/prodazha/kvartiry/?page={page_number}"
        is_empty = main(url, df, user_agent)

        # if the page is empty, scraping is stopped
        if is_empty: break

        #page_number += 1

    df.to_excel("Данные с Krisha.xlsx", index=False)

    print("Web scraping is done!")