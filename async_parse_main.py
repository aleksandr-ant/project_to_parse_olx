import asyncio
import json
import logging

import aiohttp
from bs4 import BeautifulSoup

from decorators import async_measure_time


async def parse_url(url: str, session) -> list:
    """
    Собирает ссылки на объявления и возвращает list
    :param url: url главной ссылки
    :param session: session запроса
    :return: список со ссылками на каждое объявление
    """
    url_links_kv = []
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/"
                  "avif,image/webp,image/apng,*/*;q=0.8,"
                  "application/signed-exchange;v=b3;q=0.9",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/95.0.4638.54 Safari/537.36 "
    }
    r = await session.get(url=url, headers=headers)
    result = await r.text()

    soup = BeautifulSoup(result, "lxml")

    url_kv = soup.find_all("a", class_="thumb tdnone scale1 rel detailsLink linkWithHash")
    for url_link in url_kv:
        url_link_kv = url_link.get("href").split('#')[0]
        url_links_kv.append(url_link_kv)
    return url_links_kv


async def parse_items(url: str, session) -> dict:
    """
    Принимает URL и Session, отправляет запрос на страничку и парсить объявления
    :param url: ссылка
    :param session: сессия запроса
    :return: dict
    """
    kv_dict = {}
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/"
                  "avif,image/webp,image/apng,*/*;q=0.8,"
                  "application/signed-exchange;v=b3;q=0.9",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/95.0.4638.54 Safari/537.36 "
    }
    try:
        r = await session.get(url=url, headers=headers)
        result = await r.text()
        soup = BeautifulSoup(result, "lxml")

        try:
            title = soup.find('h1', class_='css-r9zjja-Text eu5v0x0').text.strip("#")
            address = soup.find_all(class_='css-tyi2d1').pop(-1).text
            url = url
            try:
                img_url = soup.find(class_="swiper-zoom-container").find("img").get("src")
                img_url = img_url.split(";")[0]
            except Exception as ex:
                # logging.error(f'Ошибка изображения {ex}')
                print(ex)
                img_url = "https://www.openbusiness.ru/upload/iblock/72d/09876543234579.jpg"
            price = soup.find('h3', class_='css-okktvh-Text eu5v0x0').text

            wrapped = soup.find_all("li", class_="css-ox1ptj")
            item_ads = {}
            for i in wrapped:
                item = i.find("p", class_="css-xl6fe0-Text eu5v0x0").text
                if item.startswith("Бизнес") or item.startswith("Частное лицо"):
                    item_ads["type_ads"] = item
                if item.startswith("Количество комнат"):
                    item_ads["amount"] = item.split(":")[-1].strip()
                if item.startswith("Общая площадь"):
                    item_ads["area"] = item.split(":")[-1].strip()
                if item.startswith("Этаж: "):
                    item_ads["floor"] = item.split(":")[-1].strip()
                if item.startswith("Этажность дома"):
                    item_ads["floors"] = item.split(":")[-1].strip()

            kv_dict[title] = {"title": title, "url": url, "price": price, "address": address, "img_url": img_url,
                              "items_ads": item_ads}

        except Exception as ex:
            # logging.error(f'Ошибка сбора объявления {ex}')
            print(ex)
            title = "Не удалось определить Title"
            address = "Нету записи"
            img_url = "https://www.openbusiness.ru/upload/iblock/72d/09876543234579.jpg"
            price = "Не удалось оперделить цену"
            item_ads = {'type_ads': 'Нету', 'amount': 'Нету', 'area': 'Нету',
                        'floor': 'Нету', 'floors': 'Нету'}
            kv_dict[title] = {"title": title, "url": url, "price": price, "address": address, "img_url": img_url,
                              "items_ads": item_ads}
    except Exception as ex:
        # logging.error(f"Ошибка ссылки {ex}")
        print(ex)

    return kv_dict


@async_measure_time
async def main_parse_url():
    # logging.info("Начали процесс парсинга")
    print("Начали процесс парсинга")
    kv_dict = {}
    async with aiohttp.ClientSession() as session:

        url_olx = "https://www.olx.uz/nedvizhimost/kvartiry/"

        async_list_url = await asyncio.gather(
            *(parse_url(
                url=f"{url_olx}prodazha/tashkent/?view=galleryWide&page={i}",
                session=session) for i in range(1, 5)),
            *(parse_url(
                url=f"{url_olx}arenda-dolgosrochnaya/tashkent/?view=galleryWide&page={i}",
                session=session) for i in range(1, 5))
        )

        list_url = []
        for urls in async_list_url:
            for url in urls:
                list_url.append(url)

        with open('url_links_kv.txt', 'w') as file:
            for line in list_url:
                file.write(f'{line}\n')
        print("Закончили сбор ссылок")

    #connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession() as session:
        with open('url_links_kv.txt') as file:
            lines = [line.strip() for line in file.readlines()]
        # тут распаковать линии и отправить в функцию через *
        url_items_ads = await asyncio.gather(*(parse_items(url=url, session=session) for url in lines))
        print(len(url_items_ads))

        for k in url_items_ads:
            for k, v in k.items():
                kv_dict[v["title"]] = {"title": v["title"], "url": v["url"], "price": v["price"],
                                       "address": v["address"], "img_url": v["img_url"],
                                       "items_ads": v["items_ads"]}

        with open("kv_dict.json", "w", encoding='utf-8') as file:
            json.dump(kv_dict, file, ensure_ascii=False)
            # logging.info("Закончили парсинг объявлений")
            print("Закрыли json")
    print("Закончили процесс парсинга")


if __name__ == '__main__':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main_parse_url())
