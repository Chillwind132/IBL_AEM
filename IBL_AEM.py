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

class main():
    def __init__(self):
        
        self.rooturl = 'https://www-pwc-com-dpe-staging.pwc.com'  # https://www.pwc.com https://www-pwc-com-dpe-staging.pwc.com

        self.data_dict = {}
        self.snapshot = {}
        self.row_list = {}
        self.i = 0
        self.ex_cnt = 0
        self.row = ""

        init()
        self.create_csv_header()
        self.user_input()

    def user_input(self):
        if os.path.exists("data_snapshot.json"):
            print(Back.GREEN + "data_snapshot.json - FOUND!" + Style.RESET_ALL)
            selection = input("Would you like to re-generate snapshot file? Press 1 to regenerate; Press 2 to use current file\n")
            while selection != '1' and selection != '2':
                print('Invalid input')
                selection = input("Would you like to re-generate snapshot file? Press 1 to regenerate; Press 2 to use current file\n")
            if selection == '1':

                self.generate_snapshot_file()

            elif selection == '2':

                self.calculations(self.snapshot, self.row_list, self.data_dict, self.rooturl)
        else:
            print(Back.RED + "data_snapshot.json - NOT FOUND!" + Style.RESET_ALL)
            selection = input("Would you like to generate a new snapshot file from data_snapshot_input.csv? Press 1 to generate; Press 2 to Exit\n")
            while selection != '1' and selection != '2':
                print('Invalid input')
                selection = input("Would you like to generate a snapshot file from data_snapshot_input.csv? Press 1 to generate; Press 2 to continue\n")
            if selection == '1':
                self.create_json()
                self.generate_snapshot_file()

            elif selection == '2':
                sys.exit()

    def create_csv_header(self):
        refernce_header = ['URL', 'Status', 'Inbound links']
        file = open('output.csv', 'w', newline='')
        write = csv.writer(file)
        write.writerows([refernce_header])


    def calculations(self, snapshot, row_list, data_dict, rooturl):
        # We need to check if Target_url is on this page, if it is add it to the respective dict
        #Keep snapshots of the page and then iterate through them?

        snapshot = self.load_json_snapshot(snapshot)
        self.read_data(rooturl)
        for item in row_list: 
            #t = row_list[item]["Response"]
            #data_dict.setdefault(item, []).append(t)
            for key in snapshot:
                if item in snapshot[key]["List"]:
                    data_dict.setdefault(item, []).append(key)
                    #data_dict.setdefault(key, []).append(response)
                    
                    print("FOUND")

        self.populate_data(data_dict, row_list, snapshot)

    def populate_data(self, data_dict, row_list, snapshot):
        with open(r'output.csv', 'a', newline='') as f:

            writer = csv.writer(f)
            for key, value in data_dict.items():
                try:
                    response = snapshot[key]["Response"]
                except Exception:
                    response = "Response missing from DB"
                writer.writerow([key, response, value])

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
        if os.path.exists("data_snapshot_input.csv"):
            print(Back.GREEN + "data_snapshot_input.csv File Found - Generating a snapshot..." + Style.RESET_ALL)
        else:
            print(Fore.RED + "No data_snapshot_input.csv File Found. Exiting" + Style.RESET_ALL )
            time.sleep(1)
            sys.exit()

        with open('data_snapshot_input.csv', 'r') as read_obj: ### data_snapshot_input.csv 
            csv_reader = reader(read_obj)
            header = next(csv_reader)
            if header != None:
                for row in csv_reader:
                    self.row = row
                    
                    if str(row[0]) == "":
                        print("EMPTRY ROW")

                        with open("data_snapshot.json", 'w') as f:
                            json.dump(self.snapshot, f, indent=2)  

                        print (Back.GREEN + "Total nodes added to snapshot: {1} Total errors caught: {0}".format(self.ex_cnt, self.i) + Style.RESET_ALL)
                        return

                    time.sleep(1)
                    self.record_response()

                        ### Update JSON line by line in case it crashes?

    def record_response(self):
        full_url_list = []
        author_url_string = str(self.row[0])
        url_1 = author_url_string.replace("/content/pwc", self.rooturl)
        full_target_url = url_1 + ".html"

        try:
            response = requests.get(full_target_url, allow_redirects=False, timeout=10)
            soup = BeautifulSoup(response.text, 'html.parser')
            hyperlink ='a'
            for link in soup.find_all(hyperlink):
                href_link = link.get('href')

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

            if response.status_code == 301:
                response_ok = "301 redirect"
                print("REDIRECT", full_target_url)
            elif response.ok:
                self.i = self.i + 1
                response_ok = 'OK'
                print("VALID", full_target_url, "Total URLS:", self.i)
                self.snapshot[full_target_url] = {}
                self.snapshot[full_target_url]["List"] = full_url_list
                self.snapshot[full_target_url]["Response"] = response_ok
                self.snapshot[full_target_url]["Index"] = self.i
            else:
                response_ok = "Broken"
                print("INVALID", full_target_url)
                #data_dict.setdefault(full_target_url, []).append('Broken URL')    
                
            if self.i % 10 == 0: # Update JSON every N iterations
                self.update_json(self.snapshot)

        except (requests.exceptions.RequestException, ValueError) as e:
            print(Back.RED + 'Error caught!' + Style.RESET_ALL) 
            self.ex_cnt = self.ex_cnt + 1
            print(e)

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
    start = time.time()
    main()

    end = time.time()
    print(end - start)
    
    print("Done")
    

# https://www.pwc.com/ca/en/industries/entertainment-media/moving-into-multiple-business-models-download-form
#/content/pwc/ca/en/industries/entertainment-media/moving-into-multiple-business-models-download-form