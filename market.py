import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):

    """Получает список товарных предложений для указанной кампании.

        Args:
            page (str): Текущий токен страницы для постраничного доступа.
            campaign_id (str): Идентификатор кампании в Яндекс.Маркет.
            access_token (str): Токен доступа для авторизации при выполнении API-запроса.

        Returns:
            list: Список товарных предложений в формате JSON.

        Example:
            >>> get_product_list("page_token_value", "123456", "your_access_token")
            [{'offer': {'shopSku': 'SKU_1', ...}}, {'offer': {'shopSku': 'SKU_2', ...}}]

        Note:
            Убедитесь, что access_token действителен, иначе запрос вернёт ошибку.
        """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """Обновляет остатки товаров для указанной кампании в Яндекс.Маркет.

        Эта функция отправляет обновления остатков товаров в Яндекс.Маркет через API.

        Args:
            stocks (list): Список остатков товаров (SKU), которые нужно обновить.
            campaign_id (str): Идентификатор кампании в Яндекс.Маркет.
            access_token (str): Токен доступа для авторизации при выполнении API-запроса.

        Returns:
            dict: Ответ от API Яндекс.Маркет о результатах обновления остатков.

        Example:
            >>> stocks = [
            ...     {"sku": "SKU_1", "warehouseId": "warehouse_1", "items": [{"count": 10, "type": "FIT", "updatedAt": "2023-01-01T00:00:00Z"}]},
            ...     {"sku": "SKU_2", "warehouseId": "warehouse_1", "items": [{"count": 5, "type": "FIT", "updatedAt": "2023-01-01T00:00:00Z"}]}
            ... ]
            >>> update_stocks(stocks, "campaign_id_value", "your_access_token")
            {'status': 'success', 'updated_count': 2}

        Example of incorrect execution:
            >>> update_stocks([], "wrong_campaign_id", "invalid_access_token")
            requests.exceptions.HTTPError: 401 Unauthorized
        """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """Обновляет цены товаров в указанной кампании Яндекс.Маркет.

    Эта функция отправляет обновления цен на товары в Яндекс.Маркет через API.

    Args:
        prices (list): Список цен на товары, которые необходимо обновить.

    Returns:
        dict: Ответ от API Яндекс.Маркет о результатах обновления цен.

    Raises:
        requests.exceptions.HTTPError: Если запрос к API завершился с ошибкой.

    Example:
        >>> prices = [
        ...     {"id": "SKU_1", "price": {"value": 1000, "currencyId": "RUR"}},
        ...     {"id": "SKU_2", "price": {"value": 2000, "currencyId": "RUR"}}
        ... ]
        >>> update_price(prices, "campaign_id_value", "your_access_token")
        {'status': 'success', 'updated_count': 2}

    Example of incorrect execution:
        >>> update_price([], "wrong_campaign_id", "invalid_access_token")
        requests.exceptions.HTTPError: 401 Unauthorized
    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """Получает артикулы товаров из Яндекс Маркет для указанной кампании.

    Эта функция использует постраничный запрос к API Яндекс Маркет для получения
    идентификаторов предложений (shopSku) всех товаров в заданной кампании.

    Args:
        campaign_id (str): Идентификатор кампании в Яндекс Маркет, для которой необходимо получить артикулы товаров.
        market_token (str): Токен доступа для авторизации при выполнении API-запроса.

    Returns:
        list: Список артикулов товаров (shopSku), полученных из Яндекс Маркет.

    Example:
        >>> offer_ids = get_offer_ids("123456", "your_market_token")
        >>> print(offer_ids)
        ['SKU_1', 'SKU_2', 'SKU_3']

    Example of incorrect execution:
        >>> offer_ids = get_offer_ids("wrong_id", "invalid_token")
        >>> print(offer_ids)
        []
    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """Создает список остатков товаров на основе данных о наличии и идентификаторов предложений.

     Функция формирует список остатков товаров, которые нужно обновить на Яндекс Маркет.
     Если количество товара больше 10, устанавливается значение 100, если 1 — 0,
     иначе используется реальное количество.

     Args:
         watch_remnants (list): Список словарей, содержащий информацию о остатках товаров,
                                 включая их идентификаторы и количество.
         offer_ids (list): Список идентификаторов предложений (shopSku), которые уже загружены на маркет.
         warehouse_id (str): Идентификатор склада, на котором хранятся товары.

     Returns:
         list: Список, представляющий остатки товаров, которые нужно обновить,
               где каждый элемент списка является словарем с ключами:

     Example:
         >>> watch_remnants = [{"Код": "SKU_1", "Количество": "15"}, {"Код": "SKU_2", "Количество": "1"}]
         >>> offer_ids = ["SKU_1", "SKU_2", "SKU_3"]
         >>> warehouse_id = "WAREHOUSE_001"
         >>> stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
         >>> print(stocks)
         [{'sku': 'SKU_1', 'warehouseId': 'WAREHOUSE_001', 'items': [{'count': 100, 'type': 'FIT', 'updatedAt': '2023-10-01T10:00:00Z'}]},
          {'sku': 'SKU_2', 'warehouseId': 'WAREHOUSE_001', 'items': [{'count': 0, 'type': 'FIT', 'updatedAt': '2023-10-01T10:00:00Z'}]},
          {'sku': 'SKU_3', 'warehouseId': 'WAREHOUSE_001', 'items': [{'count': 0, 'type': 'FIT', 'updatedAt': '2023-10-01T10:00:00Z'}]}]

     Example of incorrect execution:
         >>> watch_remnants = []
         >>> offer_ids = []
         >>> warehouse_id = "WAREHOUSE_001"
         >>> stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
         >>> print(stocks)
         []
     """
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    # Добавим недостающее из загруженного:
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """Создает список цен на товары на основе оставшихся остатков и их идентификаторов.

    Функция формирует список цен на товары, сопоставляя идентификаторы из
    загруженных остатков с данными цен. Цена преобразуется перед добавлением
    в список.

    Args:
        watch_remnants (list): Список словарей, содержащих информацию о остатках
                                товаров, в том числе их идентификаторы и цены.
        offer_ids (list): Список идентификаторов предложений (offer_id),
                          которые уже загружены на маркет.

    Returns:
        list: Список словарей, каждый из которых представляет цену товара,
              содержащий ключи:
             
    Example:
        >>> watch_remnants = [{"Код": "SKU_1", "Цена": "5'990.00 руб."}, {"Код": "SKU_2", "Цена": "1'200.00 руб."}]
        >>> offer_ids = ["SKU_1", "SKU_2", "SKU_3"]
        >>> prices = create_prices(watch_remnants, offer_ids)
        >>> print(prices)
        [{'id': 'SKU_1', 'price': {'value': 5990, 'currencyId': 'RUR'}},
         {'id': 'SKU_2', 'price': {'value': 1200, 'currencyId': 'RUR'}}]

    Example of incorrect execution:
        >>> watch_remnants = []
        >>> offer_ids = ["SKU_1"]
        >>> prices = create_prices(watch_remnants, offer_ids)
        >>> print(prices)
        []
    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                # "feed": {"id": 0},
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    # "discountBase": 0,
                    "currencyId": "RUR",
                    # "vat": 0,
                },
                # "marketSku": 0,
                # "shopSku": "string",
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token, warehouse_id):
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        # FBS
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        # Обновить остатки FBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        # Поменять цены FBS
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        # DBS
        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        # Обновить остатки DBS
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        # Поменять цены DBS
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
