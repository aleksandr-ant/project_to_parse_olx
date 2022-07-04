import json
import requests

from bs4 import BeautifulSoup

from decorators import measure_time


def get_url_olx():
    """
    Выполняет сбор всех ссылок
    """
    print("show func get_url_olx")
    url_links_kv = []
    headers = {
        "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/"
                  "avif,image/webp,image/apng,*/*;q=0.8,"
                  "application/signed-exchange;v=b3;q=0.9",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/95.0.4638.54 Safari/537.36 "
    }
    for page_1 in range(1, 6, 1):
        url = f"https://www.olx.uz/nedvizhimost/kvartiry/prodazha/tashkent/?view=galleryWide&page={page_1}"
        r = requests.get(url=url, headers=headers)
        result = r.content

        soup = BeautifulSoup(result, "lxml")

        url_kv = soup.find_all("a", class_="thumb tdnone scale1 rel detailsLink linkWithHash")
        for url_link in url_kv:
            url_link_kv = url_link.get("href").split("#")[0]
            url_links_kv.append(url_link_kv)

    for page_2 in range(1, 6, 1):
        url = f"https://www.olx.uz/nedvizhimost/kvartiry/arenda-dolgosrochnaya/tashkent/?view=galleryWide&page={page_2}"
        r = requests.get(url=url, headers=headers)
        result = r.content

        soup = BeautifulSoup(result, "lxml")

        url_kv = soup.find_all("a", class_="thumb tdnone scale1 rel detailsLink linkWithHash")
        for url_link in url_kv:
            url_link_kv = url_link.get("href").split('#')[0]
            url_links_kv.append(url_link_kv)

    with open('url_links_kv.txt', 'w') as file:
        for line in url_links_kv:
            file.write(f'{line}\n')
        print("Закончили сбор ссылок")

    # Начинаем перебор по ссылкам

    with open('url_links_kv.txt') as file:
        lines = [line.strip() for line in file.readlines()]
    print("Начали цикл сбора квартир")
    kv_dict = {}
    for line in lines:
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/"
                      "avif,image/webp,image/apng,*/*;q=0.8,"
                      "application/signed-exchange;v=b3;q=0.9",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/95.0.4638.54 Safari/537.36 "
        }
        url = line
        r = requests.get(url=url, headers=headers)
        result = r.content

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

    print(len(kv_dict))

    with open("kv_dict.json", "w", encoding='utf-8') as file:
        json.dump(kv_dict, file, ensure_ascii=False)
        print("Закончили цикл сбора")


@measure_time
def main():
    get_url_olx()


if __name__ == "__main__":
    main()
