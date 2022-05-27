from getpass import getuser
import requests
import os
import pandas as pd
from bs4 import BeautifulSoup as bs
from concurrent.futures import ThreadPoolExecutor
from random import sample, choice

class Seeker:
    def __init__(self):
        self.proxy_url = 'https://free-proxy-list.net/'
        self.proxy_list = self.get_proxies()
        self.response = None
        self.local_proxy = None
        self.querystring = {
            "pageNum":"1",
            "pageSize":"10",
            "level":"0",
            "playerName":""
        }
        self.headers = {
            'Accept': "*/*",
            'Accept-Language': "en-US,en;q=0.9,ja-JP;q=0.8,ja;q=0.7,es-US;q=0.6,es;q=0.5",
            'Authentication': "null",
            'Connection': "keep-alive",
            'Referer': "https://m.swranking.com/",
            'Sec-Fetch-Dest': "empty",
            'Sec-Fetch-Mode': "cors",
            'Sec-Fetch-Site': "same-origin",
            'User-Agent': "",
            'sec-ch-ua': "^\^"
        }
        # gotta assign a user_agent to this session
        self.agent_filename = "user_agent_list/Chrome.txt"
        self.headers['User-Agent'] = self.getUserAgent(self.agent_filename)

    def getUserAgent(self, filename):
        filepath = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), filename))
        with open(filepath, 'r') as f:
            user_agent = choice(list(map(str.strip, f.readlines())))
        return user_agent

    def get_proxies(self):
        data = []
        soup = bs(requests.get(self.proxy_url).text, 'html.parser')
        table = soup.find_all('table')[0]
        # gets all headers from the dataset
        headers =[ header.text for header in table.find_all('tr')[0].find_all('th') ]
        # gets dataset contents
        for row in table.find_all('tr')[1:]:
            sub_data = [elem.text for elem in row.find_all('td')] 
            data.append(sub_data)
        # change df to use only important content
        df = pd.DataFrame(data=data, columns=headers)
        df = df[df['Anonymity'] == 'elite proxy']
        ip_list = df['IP Address']+':'+df['Port'].to_list()

        # shuffle the list of proxies
        return sample(list(ip_list), k=len(list(ip_list)))

    def test_proxy(self, proxy, procedure_url="https://m.swranking.com/api/player/replayallist"):
        if not self.response and not self.local_proxy:
            try:
                self.response = requests.get(   url=procedure_url, 
                                                headers=self.headers,
                                                params=self.querystring,
                                                proxies={'http': proxy, 'https': proxy}, 
                                                timeout=10)
                self.local_proxy = proxy
            except: pass # if request fails it will just move to the next proxy
        
    def seek(self, proxies):
        print(f'Seeking connection... on {len(proxies)} proxies')
        # thread on workers the size of the list of proxies ;?
        with ThreadPoolExecutor(max_workers=len(proxies)) as executor:
            executor.map(self.test_proxy, proxies)
        if self.response:
            print(f'Connection found @ {self.local_proxy}')
        else:
            print("No connections available...")