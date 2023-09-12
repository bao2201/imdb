from bs4 import BeautifulSoup as soup
import requests
import pandas as pd
import numpy as np

def get_link():
    url='https://vietstock.vn/'
    headers={'User-agent': 'Chrome/101.0.4951.54'}
    request=requests.get(url, headers=headers)
    html=request.content
    s=soup(html, 'html.parser')
    h4_tags=s.find_all('h4')
    urls=[]
    for h4_tag in h4_tags:
        a_tag=h4_tag.find('a')
        if a_tag is not None:
            url=a_tag['href']
            urls.append(url)
    df=pd.DataFrame(urls)
    df.to_csv(r'C:\Users\Admins\Desktop\url_list.csv')
    return
def get_table():
    url='https://s.cafef.vn/bao-cao-tai-chinh/hpg/incsta/2023/1/0/0/ket-qua-hoat-dong-kinh-doanh-cong-ty-co-phan-tap-doan-hoa-phat.chn'
    headers={'User-agent': 'Chrome/101.0.4951.54'}
    request=requests.get(url, headers=headers)
    html=request.content
    s=soup(html, 'html.parser')
    table = s.find('table')
    data = []
    for row in table.find_all('tr'):
        cols = row.find_all('td')
        if len(cols) == 0:
            cols = row.find_all('th')
        cols = [ele.text.strip() for ele in cols]
        data.append([ele for ele in cols if ele]) 
    df=pd.DataFrame(data)
    df.to_csv(r'C:\Users\Admins\Desktop\hpg.csv')
    return
def main():
    get_link()
    get_table()
    return
if __name__ == "__main__":
    main()