import random
import requests
import time
import asyncio
import aiohttp
from pyppeteer import launch
import urllib3
import socket
import logging
from logging.config import fileConfig
import re
from datetime import datetime, timedelta
import csv


# Eliminamos los warnings molestos de la navegación insegura sin SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


fileConfig('logging_config.ini')
logger = logging.getLogger()


user_agent = random.choice(requests.get('https://cdn.jsdelivr.net/gh/fijimunkii/real-user-agent@master/ua.json', verify=True, timeout=5).json())
#user_agent = UserAgent()

headers = {
        "User-Agent":user_agent,
        "Accept":"application/json, text/javascript, */*; q=0.01",
        "Accept-encoding":"gzip, deflate, br",
        "Accept-Language":"es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3",
        "Cache-Control":"no-cache",
        "Connection":"keep-alive",
        "Host":"www.pap.hacienda.gob.es",
        "Pragma":"no-cache",
        "Sec-Fetch-Dest":"empty",
        "Sec-Fetch-Mode":"cors",
        "Sec-Fetch-Site":"same-origin",
        "X-Requested-With":"XMLHttpRequest"
}


async def gather_with_concurrency(n, *tasks):
    semaphore = asyncio.Semaphore(n)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(sem_task(task) for task in tasks))



# Obtenemos las cookies adecuadas según el rango de fechas de búsqueda utilizado
async def obtain_cookies(fromdate, todate):
    browser = await launch(headless=True, ignoreHTTPSErrors=True,
                           args=['--disable-blink-features=AutomationControlled'],
                          )
    page = await browser.newPage()
    page.setDefaultNavigationTimeout(1000)

    page = (await browser.pages())[0]
    await page.setUserAgent(user_agent)
    await page.goto("https://www.pap.hacienda.gob.es/bdnstrans/GE/es/concesiones")

    # Desde - hasta
    await page.type('#fecDesde', fromdate)
    await page.type('#fecHasta', todate)

    # es necesario hacer la búsqueda para que nos salgan las cookies adecuadas
    await page.click('[title="Buscar"]')
    cookies = await page.cookies()
    await browser.close()

    cookies_header = ""
    for cookie in cookies:
      cookies_header += cookie['name'] + "=" + cookie['value'] + ";"

    return cookies_header


# Procesamiento concurrente #
async def get_async(page, url_template, csv_writer, session, errors):
    url = re.sub("__PAGE__", str(page), url_template)
    url = re.sub("__EPOCH__", str(int(time.time())), url)
    try:
      async with session.get(url, timeout=240) as response:
        data = await response.json()

        # Escribir a Csv #
        for row in data['rows']:
           row = [str(s).replace('\n',' ') for s in row]   ### Eliminamos caracter salto de línea y lo convertimos en espacio
           csv_writer.writerow(row)
        logger.info("Finalizada página [ " + str(page) + " ]")

    except TypeError:
       logger.warning("Pagina [ " + str(page) + " ] no ha podido ser capturada porque los datos devueltos están vacíos")
       errors[page] = ""
    except aiohttp.client_exceptions.ClientConnectorError:
      logger.warning("Pagina [ " + str(page) + " ] no ha podido ser capturada por problemas de conexión")
      errors[page] = ""
    except asyncio.exceptions.TimeoutError:
      logger.warning("Pagina [ " + str(page) + " ] no ha podido ser capturada por timeout de conexión")
      errors[page] = ""
    except aiohttp.client_exceptions.ClientOSError:
      logger.warning("Pagina [ " + str(page) + " ] no ha podido ser capturada por excepción ClientOSError")
      errors[page] = ""
    except requests.exceptions.ReadTimeout:
      logger.warning("Pagina [ " + str(page) + " ] no ha podido ser capturada por excepción ReadTimeout")
      errors[page] = ""


# Función para reprocesar páginas en error #
async def retry_errors(conc_req, url_template, csv_writer, session, errors):
      errors2 = {}
      logger.info("Reintentamos los errores [ " + str(errors.keys()) + " ]")
      await gather_with_concurrency(conc_req, *[get_async(page, url_template, csv_writer, session, errors2) for page in errors.keys()])
      return errors2



async def get_data(fromdate, todate, csv_writer):
  # Obtenemos las cookies
  cookies = await obtain_cookies(fromdate, todate)
  headers.update({'Cookie':cookies})

  # Definimos la url añadiendo epoch time
  url_template = 'https://www.pap.hacienda.gob.es/bdnstrans/busqueda?type=concs&_search=false&nd=__EPOCH__&rows=200&page=__PAGE__&sidx=8&sord=desc'

  # Creamos una sesion asyncio
  aioconn = aiohttp.TCPConnector(limit=None, ttl_dns_cache=300, ssl=False, force_close=True)   ### Forzamos keep-alive = false puesto que el servidor corta las sesiones
  session = aiohttp.ClientSession(connector=aioconn, headers=headers)
  errors = {}
  errors2 = {}

  # Scrap con un timeout alto y sin verificar SSL
  data = []

  # Escaneamos la primera página y obtenemos el número de páginas a descargar
  retry=0
  while 'rows' not in data or data['records'] == 0 and retry < 10:
    url = re.sub("__PAGE__", "1", url_template)
    url = re.sub("__EPOCH__", str(int(time.time())), url)
    data = requests.get(url, headers=headers, verify=False, timeout=3600).json()

    if 'rows' not in data:
      logger.debug("reintentamos conexión...")
      retry+=1
      continue
    if data['records'] == 0:
      logger.debug("reintentamos conexión...")
      retry+=1
      continue

    logger.debug("Hay un total de [ " + str(data['total']) + " ] páginas para la fecha [ " + str(todate) + " ]")

    # Volcamos los datos
    for row in data['rows']:
      row = [str(s).replace('\n',' ') for s in row]   ### Eliminamos caracter salto de línea y lo convertimos en espacio
      csv_writer.writerow(row)

    # Creamos un vector de páginas a procesar en la fecha
    pages = range(1,data['total']+1)

    # Número de procesos concurrentes
    conc_req = 5
    await gather_with_concurrency(conc_req, *[get_async(page, url_template, csv_writer, session, errors) for page in pages])

    # Reprocesamos los errores
    while len(errors.keys()) > 0:
      errors = await retry_errors(conc_req, url_template, csv_writer, session, error)


  logger.info("Procesados un total de [ " + str(data['records']) + " ] registros para la fecha [ " + str(todate) + " ]")

  # cerramos conexiones
  await session.close()



### INICIO ##

 # conversión Json a CSV. Se usan quotes mínimos
data_file = open('concesiones.csv', 'w')
csv_writer = csv.writer(data_file) #, quoting=csv.QUOTE_NONNUMERIC, escapechar='\n')


# Creamos un vector de fechas y vamos restando un día
for i in range(1, 2000):
  d = (datetime.today() - timedelta(days=i)).strftime("%d/%m/%Y")
  #d = (datetime.strptime('17/12/2021', '%d/%m/%Y') - timedelta(days=i)).strftime("%d/%m/%Y")

  logger.info("Procesando día: [ " + str(d) + " ]")
  asyncio.run(get_data(d, d, csv_writer))


data_file.close()
