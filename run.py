import aiohttp
import asyncio

from bs4 import BeautifulSoup

from config import WEB_URL, db_conn, _key

conn, cursor = db_conn()


def get_text(method):
    try:
        return method.text
    except:
        return ''


def _compose_web_url(url):
    url = url[45:-12]
    url = url.replace("\'", '').replace('+', '').replace('"', '')
    return url[:url.find('>')]


def field(item):
    text = get_text(item)
    key = get_text(item.find('b'))
    if key.lower().strip('\n') == 'интернет сайт':
        return key.strip('\n'), _compose_web_url(get_text(item.find('script')))
    # key = text.split(' ')[0]
    return key.strip('\n'), text[len(key)+1:].strip(' ').strip('\n')


def push_data(text, url):
    soup = BeautifulSoup(text, "lxml")
    content = soup.find("td", class_="tdcont")
    data = {'school_name': get_text(content.find("h1")),}
    for item in content.findAll('div')[1:-2]:
        key, value = field(item)
        try:
            data[_key(key)] = value
        except Exception as _er:
            print(url, _er)
    data['url'] = url
    keys = list(data.keys())
    sql_insert = f'INSERT INTO schools ({", ".join(keys)}) VALUES ({", ".join(["?"] * len(keys))})'
    try:
        cursor.execute(sql_insert, tuple([data[key] for key in keys]))
    except Exception as _er:
        raise _er
    try:
        conn.commit()
    except Exception as _er:
        print(_er)


async def page(i):
    url = WEB_URL.format(school_id=i)
    cursor.execute("SELECT * FROM schools WHERE url=?", (url,))
    rows = cursor.fetchall()
    if not rows:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                if resp.status == 200:
                    push_data(await resp.text(), url)
    #else:
    #    print(url)


def main():
    loop = asyncio.get_event_loop()
    tasks = [loop.create_task(page(str(i))) for i in range(1, 1000)]
    wait_tasks = asyncio.wait(tasks)
    loop.run_until_complete(wait_tasks)
    loop.close()


if __name__ == '__main__':
    main()
