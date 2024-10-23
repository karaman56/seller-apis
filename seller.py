import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
     """Получить список товаров магазина Озон.

    Эта функция выполняет запрос к API Озон для получения списка
    товаров магазина, основываясь на указанном last_id. Запрос возвращает
    товары с заданными условиями фильтрации.

    Args:
        last_id (str): Идентификатор последнего товара, с которого следует начать
                        выборку (например, для постраничного доступа).
        client_id (str): Идентификатор клиента для аутентификации API.
        seller_token (str): API-ключ продавца для аутентификации.

    Raises:
        requests.exceptions.HTTPError: Если запрос завершился неуспешно.

    Example:
        >>> products = get_product_list("", "your_client_id", "your_seller_token")
        >>> len(products) > 0
        True

    Note:
        Запрос может вернуть не более 1000 товаров за один раз.
    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """Получить список артикулов (offer_id) товаров магазина Озон.

    Эта функция выполняет запрос к API Озон для получения всех артикулов товаров
    в магазине, используя постраничную выборку. Функция продолжает запрашивать
    товары до тех пор, пока не будут получены все артикулы.

    Args:
        client_id (str): Идентификатор клиента для аутентификации API.
        seller_token (str): API-ключ продавца для аутентификации.

    Returns:
        list: Список артикулов товаров (offer_id). Каждый артикул представлен в виде строки.

    Example:
        >>> offer_ids = get_offer_ids("your_client_id", "your_seller_token")
        >>> isinstance(offer_ids, list)
        True
        >>> len(offer_ids) > 0
        True

    Note:
        Если товаров в магазине нет, возвращаемый список будет пустым.
    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """Обновить цены товаров на Озон.

        Эта функция отправляет запрос к API Озон для обновления цен на указанные товары.
        Цены должны быть предоставлены в виде списка словарей, где каждый словарь содержит
        информацию о цене и артикуле товара.

        Args:
            prices (list): Список цен на товары. Каждый элемент списка должен быть словарем
                           в формате:
                           {
                               "auto_action_enabled": str,  # статус действия
                               "currency_code": str,        # код валюты, например, "RUB"
                               "offer_id": str,             # артикул товара
                               "old_price": str,            # старая цена
                               "price": str,                # новая цена
                           }
            client_id (str): Идентификатор клиента для аутентификации API.
            seller_token (str): API-ключ продавца для аутентификации.

        Returns:
            dict: Ответ от API, содержащий информацию об успешности обновления цен.

        Example:
            >>> prices_to_update = [
            ...     {
            ...         "auto_action_enabled": "UNKNOWN",
            ...         "currency_code": "RUB",
            ...         "offer_id": "12345",
            ...         "old_price": "500",
            ...         "price": "550"
            ...     }
            ... ]
            >>> response = update_price(prices_to_update, "your_client_id", "your_seller_token")
            >>> response["success"]
            True

        Note:
            Если в списке prices не содержится действительных данных, функция может вызвать ошибку.
        """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """Обновить остатки товаров на Озон.

       Эта функция отправляет запрос к API Озон для обновления остатков указанных товаров.
       Остатки должны быть предоставлены в виде списка словарей, где каждый словарь содержит
       информацию о товаре и его количестве.

       Args:
           stocks (list): Список остатков товаров. Каждый элемент списка должен быть словарем

       Returns:
           dict: Ответ от API, содержащий информацию об успешности обновления остатков.

       Example:
           >>> stocks_to_update = [
           ...     {
           ...         "offer_id": "12345",
           ...         "stock": 50
           ...     },
           ...     {
           ...         "offer_id": "67890",
           ...         "stock": 0
           ...     }
           ... ]
           >>> response = update_stocks(stocks_to_update, "your_client_id", "your_seller_token")
           >>> response["success"]
           True

       Note:
           Если в списке stocks не содержится действительных данных,
           функция может вызвать ошибку.
       """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """Скачать файл с остатками товаров и вернуть их в виде списка.

       Эта функция загружает ZIP-архив с остатками товаров с заданного URL,
       распаковывает его и считывает данные об остатках из файла Excel.
       Возвращает список записей остатков с информацией о каждом товаре.

       Returns:
           list: Список словарей, где каждый словарь содержит информацию о товаре,

       Example:
           >>> stock_data = download_stock()
           >>> len(stock_data) > 0
           True

       Example of incorrect use:
           >>> download_stock()
           # Если файл не доступен, возникнет ошибка HTTPError.
       """
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """Создает список остатков для обновления на основе данных о товарах.

        Эта функция принимает список остатков (watch_remnants) и набор идентификаторов
        предложений (offer_ids), чтобы сформировать список остатков. Если количество товара
        больше 10, устанавливается значение 100, если 1, то 0. Для незагруженных идентификаторов
        предложений добавляется запись с остатком 0.

        Args:
            watch_remnants (list): Список словарей с информацией о товарах, где каждый словарь
                                   содержит как минимум "Код" и "Количество".

            offer_ids (list): Список строк с идентификаторами предложений, которые уже загружены
                              в систему продавцов.

        Returns:
            list: Список словарей, содержащих пары "offer_id" и "stock", где "offer_id" — это
                  идентификатор товара, а "stock" — количество.

        Example:
            >>> watch_remnants = [{"Код": "12345", "Количество": ">10"}, {"Код": "67890", "Количество": "1"}]
            >>> offer_ids = ["12345", "67890", "11111"]
            >>> create_stocks(watch_remnants, offer_ids)
            [{'offer_id': '12345', 'stock': 100}, {'offer_id': '67890', 'stock': 0}, {'offer_id': '11111', 'stock': 0}]

        Example of incorrect use:
            >>> create_stocks([], ["12345"])
            []
        """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает список цен на товары для обновления.

    Эта функция формирует список цен для товаров на основе переданных данных об остатках
    (watch_remnants) и идентификаторов товаров (offer_ids). Для каждого остатка добавляется
    информация о цене и других атрибутах.

    Args:
        watch_remnants (list): Список словарей, содержащих информацию о товарах, где каждый
                               словарь должен содержать как минимум "Код" и "Цена".

        offer_ids (list): Список строк с идентификаторами предложений, для которых необходимо
                          установить цены.

    Returns:
        list: Список словарей, содержащих данные о ценах для обновления. Каждый словарь
              имеет структуру:

    Example:
        >>> create_prices(watch_remnants, offer_ids)
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '12345', 'old_price': '0', 'price': '5990'},
         {'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '67890', 'old_price': '0', 'price': '1250'}]

    Example of incorrect use:
        >>> create_prices([], ["12345"])
        []
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
      """Преобразовать цену из строки в целое число.

    Преобразует строковое представление цены, удаляя все символы,
    кроме чисел. Ожидается, что цена может содержать символы валюты и
    разделители (например, запятые или пробелы), которые будут удалены.

    Args:
        price (str): Строка, представляющая цену. Например, "5'990.00 руб."

    Returns:
        str: Цена в формате целого числа без символов и разделителей.
             Пример: "5990".

    Example:
        >>> price_conversion("5'990.00 руб.")
        '5990'

    Raises:
        ValueError: Если input не является строкой.
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """Разделяет список на части заданного размера.

    Эта функция принимает список и делит его на подсписки, каждый из которых содержит не более
    чем n элементов. Если длина списка не делится на n, последний подсписок может содержать
    меньше элементов.

    Args:
        lst (list): Список, который необходимо разделить.
        n (int): Максимальное количество элементов в каждом подсписке.

    Yields:
        list: Подсписки, содержащие элементы из исходного списка.

    Example:
        >>> list(divide([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]

    Example of incorrect use:
        >>> list(divide([], 2))
        []
    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
