import requests as req
from bs4 import BeautifulSoup as bes
import sqlite3 as sql
from tqdm import tqdm


conn = sql.connect('habr.db')
cur = conn.cursor()
cur.execute('select * from cont order by date DESC')
last_date = cur.fetchall()[0][0]


def main_soup(main_url):
    try:
        one_get = req.get(main_url)
        one_get.encoding='utf=8'
        soup = bes(one_get.text,'lxml')
        return soup
    except Exception as x:
        return {'error':x}


def all_snippet_link(soup,tag,**kwargs):
    m_list = ['https://habr.com'+i['href'] for i in soup.find_all(tag,**kwargs)]
    return m_list


def body_version(link):
    one = main_soup(link)
    body_vers = 'article-formatted-body article-formatted-body article-formatted-body_version-1'
    try:
        one.find('div',{body_vers}).text
        return body_vers
    except Exception:
        body_vers = body_vers.replace('1','2')
        return body_vers


def snippet_cont(link_snippet):
    result = []
    for link in tqdm(link_snippet):
        one = main_soup(link)
        date = one.find('div',{"tm-article-snippet__meta"}).find_all('span')[-1].find('time')['title'].replace(',','')
        title = one.find('title').text.rstrip()
        cont = one.find('div',{body_version(link)}).text
        result += [(date,title,cont)]
    return result


def snippet_img(link_snippet):
    all_link_img = []
    for link in tqdm(link_snippet):
        one = main_soup(link)
        date = one.find('div',{"tm-article-snippet__meta"}).find_all('span')[-1].find('time')['title'].replace(',','')
        cont_img = one.find('div',{body_version(link)}).find_all('img')
        clear_img = [i['src'] for i in cont_img]
        all_link_img += [(date,clear_img)]
    return all_link_img


def insert_cont(result_cont):
    for i in result_cont:
        cur.execute("""insert into cont (date, title, content) values (?,?,?)""", i)
    conn.commit()
    return


def insert_img(result_img):
    for i in result_img:
        for x in i[1]:
            r_get = req.get(x)
            img_open = open('img.jpg','wb')
            img_open.write(r_get.content)
            img_open.close()
            with open('img.jpg','rb') as new_img:
                byte = new_img.read()
                cur.execute("""insert into img (cont_id, img) values (?,?)""", (i[0], sql.Binary(byte)))
            conn.commit()
    return


def update(result_cont,result_img):
    fin_cont = []
    fin_img = []
    for i in range(len(result_cont)):
        if result_cont[i][0]==last_date:
            fin_cont = result_cont[:i]
            insert_cont(fin_cont)
    for x in tqdm(range(len(result_img))):
        if result_img[x][0]==last_date:
            fin_img = result_img[:x]
            insert_img(fin_img)


if __name__ == '__main__':
    main_url = 'https://habr.com/ru/hub/python/'
    big_soup = main_soup(main_url)
    link_snippet = all_snippet_link(big_soup,'a',**{'class':'tm-article-snippet__title-link'})
    result_cont = snippet_cont(link_snippet[:10])
    result_img = snippet_img(link_snippet[:10])
    update_base = update(result_cont,result_img)
    cur.close()