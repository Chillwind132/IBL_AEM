from csv import reader
from logging import exception
from os import read
import time
import requests
from bs4 import BeautifulSoup
import csv  
import time
import os.path
import json
import sys
from colorama import Fore, Back, Style
from colorama import init
import concurrent.futures

class main():
    def __init__(self):
        
        self.rooturl = 'https://www-pwc-com-dpe-staging.pwc.com'  # https://www.pwc.com https://www-pwc-com-dpe-staging.pwc.com

        self.data_dict = {}
        self.snapshot = {}
        self.row_list = {}
        self.list_urls_updated =[]
        self.i = 0
        self.ex_cnt = 0
        self.found_cnt = 0
        self.redirect_i = 0
        self.broken_i = 0
        self.row = ""
        self.selection = ""
        self.selection_2 = ""
        self.selection_iGet = ""

        self.main()
        
    def main(self):
        
        init()
        self.create_csv_header()
        self.user_input()

        if self.selection == '1':
            t0 = time.time()
            self.generate_snapshot_file()
            t1 = time.time()
            self.update_json(self.snapshot)
            print(Back.BLUE + f"{t1-t0} seconds to record {len(self.snapshot)} URLs." + Style.RESET_ALL) 
            print (Back.GREEN + f"Total nodes added to snapshot: {self.i} Total redirects: {self.redirect_i} Total broken: {self.broken_i} Total errors caught: {self.ex_cnt}" + Style.RESET_ALL)
            
        elif self.selection == '2':
            
            self.calculations(self.snapshot, self.row_list, self.data_dict, self.rooturl)

        if self.selection_2 == '1':
            self.create_json()
            t0 = time.time()
            self.generate_snapshot_file()
            t1 = time.time()
            self.update_json(self.snapshot)
            print(Back.BLUE + f"{t1-t0} seconds to record {len(self.snapshot)} URLs." + Style.RESET_ALL) 
            print(Back.GREEN + f"Total nodes added to snapshot: {self.i} Total redirects: {self.redirect_i} Total broken: {self.broken_i} Total errors caught: {self.ex_cnt}" + Style.RESET_)
            
        elif self.selection_2 == '2':
            sys.exit()
        
        print("Done")

    def user_input(self):
        if os.path.exists("data_snapshot.json"):
            print(Back.GREEN + "data_snapshot.json - FOUND!" + Style.RESET_ALL)
            self.selection = input("Would you like to re-generate snapshot file? Press 1 to regenerate; Press 2 to use current file\n")
            while self.selection != '1' and self.selection != '2':
                print('Invalid input')
                self.selection = input("Would you like to re-generate snapshot file? Press 1 to regenerate; Press 2 to use current file\n")
            self.selection_iGet = input("Would you like to record GET responses for unknown URLs? Press 1 to record; Press 2 skip\n")
            while self.selection_iGet != '1' and self.selection_iGet != '2':
                print('Invalid input')
                self.selection_iGet = input("Would you like to record GET responses for unknown URLs? Press 1 to record; Press 2 skip\n")
        elif os.path.exists("data_snapshot.json") == False:
            print(Back.RED + "data_snapshot.json - NOT FOUND!" + Style.RESET_ALL)
            self.selection_2 = input("Would you like to generate a new snapshot file from data_snapshot_input.csv? Press 1 to generate; Press 2 to Exit\n")
            while self.selection_2 != '1' and self.selection_2 != '2':
                print('Invalid input')
                self.selection_2 = input("Would you like to generate a snapshot file from data_snapshot_input.csv? Press 1 to generate; Press 2 to continue\n")
            
    def create_csv_header(self):
        refernce_header = ['Outbound reference', 'Status', 'Source URLs']
        file = open('output.csv', 'w', newline='')
        write = csv.writer(file)
        write.writerows([refernce_header])

    def calculations(self, snapshot, row_list, data_dict, rooturl):
        # We need to check if Target_url is on this page, if it is add it to the respective dict
        #Keep snapshots of the page and then iterate through them?
        t0 = time.time()
        snapshot = self.load_json_snapshot(snapshot)
        self.read_data(rooturl)
        for item in row_list: 
            #t = row_list[item]["Response"]
            #data_dict.setdefault(item, []).append(t)
            for key in snapshot:
                if item in snapshot[key]["List"]:
                    data_dict.setdefault(item, []).append(key)
                    #data_dict.setdefault(key, []).append(response)
                    
                    print(f"FOUND - {key}")
                    self.found_cnt = self.found_cnt + 1

        self.populate_data(data_dict, row_list, snapshot)
        t1 = time.time()
        print(Back.BLUE + f"{t1-t0} seconds to find {self.found_cnt} matches. Total exceptions: {self.ex_cnt}" + Style.RESET_ALL)

    def populate_data(self, data_dict, row_list, snapshot):
        with open(r'output.csv', 'a', newline='') as f:

            writer = csv.writer(f)
            for key, value in data_dict.items():
                try:
                    response = snapshot[key]["Response"]
                    if self.selection_iGet == "1":
                        print(Back.GREEN + "HTTP GET response code found" + Style.RESET_ALL)
                except Exception:
                    response = "HTTP GET response code missing from DB"
                    #time.sleep(3)
                    if self.selection_iGet == "1":
                        print(Back.BLUE + "Recording HTTP GET response code..." + Style.RESET_ALL)
                        response = self.get_response(key)
                    
                    #writer.writerow([key, response_2, value])
                writer.writerow([key, response, value])

    def get_response(self, url):

        try:
            response = requests.get(url, allow_redirects=False, timeout=10)
            return response.status_code
        except (requests.exceptions.RequestException, ValueError) as e:
            print(Back.RED + 'Error caught!\n' + Style.RESET_ALL) 
            self.ex_cnt = self.ex_cnt + 1
            print(f"{e}\n")
            return e

    def read_data(self, rooturl):
        with open('urls_to_find.csv', 'r') as read_obj:
            exit = False
            csv_reader = reader(read_obj)
            header = next(csv_reader)
            if header != None:
                for row in csv_reader:

                    if str(row[0]) == "":
                        print("EMPTRY ROW")
                        self.calculations(self.snapshot, self.row_list, self.data_dict)
                        self.populate_data(self.data_dict, self.snapshot,
                                      full_target_url, self.row_list)
                        return

                    author_url_string = str(row[0])
                    url_1 = author_url_string.replace("/content/pwc", rooturl)
                    full_target_url = url_1 + ".html"

                    self.row_list[full_target_url] = {}

                    if full_target_url in self.snapshot: ## remove this?
                        self.row_list[full_target_url]["Response"] = self.snapshot[full_target_url]["Response"]
                        print("FOUND RESPONSE IN SNAPSHOT. APPENDING>>>")

    def generate_snapshot_file(self):

        MAX_THREADS = 5 # Number of parallel threads

        if os.path.exists("data_snapshot_input.csv"):
            print(Back.GREEN + "data_snapshot_input.csv File Found - Generating a snapshot..." + Style.RESET_ALL)
        else:
            print(Fore.RED + "No data_snapshot_input.csv File Found. Exiting" + Style.RESET_ALL )
            
            sys.exit()
        
        with open('data_snapshot_input.csv', 'r') as read_obj: ### data_snapshot_input.csv 
            for row in csv.reader(read_obj):    
                author_url_string = row[0]
                url_1 = author_url_string.replace("/content/pwc", self.rooturl)
                if url_1 != "":
                    full_target_url = url_1 + ".html"
                    self.list_urls_updated.append(full_target_url)

            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
                executor.map(self.record_response, self.list_urls_updated)  #
                executor.shutdown(wait=True)
                    
    def record_response(self, url):
        full_url_list = []
        
        try:
            response = requests.get(url, allow_redirects=False, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            hyperlink ='a'
            for link in soup.find_all(hyperlink):
                href_link = link.get('href')
                ### probably not needed
                if href_link is not None and href_link.startswith("content/pwc"):
                    href_link_2 = href_link
                    href_link_1 = href_link.replace("content/pwc","https://www.pwc.com")
                    href_link =  href_link_1.replace(".html","/") + href_link_2 # checks for https://www.pwc.com/ca/en/services/tax/budgets/2021/content/pwc/ca/en/services/tax/budgets/2021.html
                elif href_link is not None and href_link.startswith("/content/pwc"):
                    href_link =  self.rooturl + href_link.replace("/content/pwc", "") + url
                ###
                try:
                    if href_link[0] == "/":
                        
                        try:
                            if href_link[1] == "/":
                                link_updated = href_link[1:]

                                href_link = self.rooturl + link_updated
                            else:
                                href_link = self.rooturl + href_link
                        except Exception:
                                href_link = self.rooturl + href_link
                    else:
                        direction = "External"
                except Exception:
                    t = 0
                
                if href_link != '#':
                    full_url_list.append(href_link) ## Populate URL list for each node 

            time.sleep(3)

            if response.status_code == 301:
                response_ok = "301 redirect"
                print("REDIRECT\n", url)
                self.redirect_i = self.redirect_i + 1
            elif response.ok:
                self.i = self.i + 1
                response_ok = 'OK'
                print(f"VALID: {url} Total URLS: {self.i}\n")
                self.snapshot[url] = {}
                self.snapshot[url]["List"] = full_url_list
                self.snapshot[url]["Response"] = response_ok
                self.snapshot[url]["Index"] = self.i
            else:
                response_ok = "Broken"
                print(f"INVALID: {url}\n")
                self.broken_i = self.broken_i + 1 
                
            #if self.i % 10 == 0: # Update JSON every N iterations
                #self.update_json(self.snapshot)

        except (requests.exceptions.RequestException, ValueError) as e:
            print(Back.RED + 'Error caught!\n' + Style.RESET_ALL) 
            self.ex_cnt = self.ex_cnt + 1
            print(f"{e}\n")

    def create_json(self):
        with open('data_snapshot.json', 'w') as f:
            json.dump("", f, indent=2)

    def update_json(self, snapshot ):

        with open('data_snapshot.json') as f:
            data = json.load(f)

        with open('data_snapshot.json', 'w') as f:
            json.dump(snapshot, f, indent=2)

    def load_json_snapshot(self, snapshot):

        with open('data_snapshot.json', 'r') as f:
            snapshot = json.load(f)
        return snapshot

if __name__ == "__main__":
    main()

    
    
    
    

# https://www.pwc.com/ca/en/industries/entertainment-media/moving-into-multiple-business-models-download-form
#/content/pwc/ca/en/industries/entertainment-media/moving-into-multiple-business-models-download-form