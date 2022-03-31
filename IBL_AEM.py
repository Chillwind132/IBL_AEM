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


def create_csv_header():
    refernce_header = ['URL', 'Status', 'Inbound links']
    file = open('output.csv', 'w', newline='')
    write = csv.writer(file)
    write.writerows([refernce_header])


def calculations(snapshot, row_list, data_dict, rooturl):
    # We need to check if Target_url is on this page, if it is add it to the respective dict
    #Keep snapshots of the page and then iterate through them?

    snapshot = load_json_snapshot(snapshot)
    read_data(rooturl)
    for item in row_list: 
        #t = row_list[item]["Response"]
        #data_dict.setdefault(item, []).append(t)
        for key in snapshot:
            if item in snapshot[key]["List"]:
                data_dict.setdefault(item, []).append(key)
                #data_dict.setdefault(key, []).append(response)
                
                print("FOUND")
    populate_data(data_dict, row_list, snapshot)
                
def populate_data(data_dict, row_list, snapshot):
    with open(r'output.csv', 'a', newline='') as f:
        
        writer = csv.writer(f)
        for key, value in data_dict.items():
            try:
                response = snapshot[key]["Response"]
            except Exception:
                response = "Response missing from DB"
            writer.writerow([key, response, value])
                
def read_data(rooturl):
    with open('urls_to_find.csv', 'r') as read_obj:
        exit = False
        csv_reader = reader(read_obj)
        header = next(csv_reader)
        if header != None:
            for row in csv_reader:
                
                if str(row[0]) == "":
                    print("EMPTRY ROW")
                    calculations(snapshot, row_list, data_dict)
                    populate_data(data_dict, snapshot,
                                  full_target_url, row_list)
                    return

                full_url_list = []
                author_url_string = str(row[0])
                url_1 = author_url_string.replace("/content/pwc", rooturl)
                full_target_url = url_1 + ".html"

                row_list[full_target_url] = {}

                if full_target_url in snapshot:
                    row_list[full_target_url]["Response"] = snapshot[full_target_url]["Response"]
                    print("FOUND RESPONSE IN SNAPSHOT. APPENDING>>>")

def generate_snapshot_file():
    if os.path.exists("data_snapshot_input.csv"):
        print("data_snapshot_input.csv File Found - Generating a snapshot...")
    else:
        print("No data_snapshot_input.csv File Found. Exiting")
        time.sleep(1)
        sys.exit()
        
    i = 0
    with open('data_snapshot_input.csv', 'r') as read_obj: ### data_snapshot_input.csv 
        csv_reader = reader(read_obj)
        header = next(csv_reader)
        if header != None:
            for row in csv_reader:
                
                time.sleep(1)
                if str(row[0]) == "":
                    print("EMPTRY ROW")
                    return

                full_url_list = []
                author_url_string = str(row[0])
                url_1 = author_url_string.replace("/content/pwc", rooturl)
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

                                        href_link = rooturl + link_updated
                                    else:
                                        href_link = rooturl + href_link
                                except Exception:
                                        href_link = rooturl + href_link
                            else:
                                direction = "External"
                        except Exception:
                            t = 0
                    
                        full_url_list.append(href_link) ## Populate URL list for each node

                    if response.status_code == 301:
                        response_ok = "301 redirect"
                        print("REDIRECT", full_target_url)
                    elif response.ok:
                        i = i + 1
                        response_ok = 'OK'
                        print("VALID", full_target_url, "Total URLS:", i)
                        snapshot[full_target_url] = {}
                        snapshot[full_target_url]["List"] = full_url_list
                        snapshot[full_target_url]["Response"] = response_ok
                        
                    else:
                        response_ok = "Broken"
                        print("INVALID", full_target_url)
                        #data_dict.setdefault(full_target_url, []).append('Broken URL')

                except Exception: #ReadTimeout
                    print("Timeout Exception")

                    ### Update JSON line by line in case it crashes?
                    
def load_json_snapshot(snapshot):

    with open('data_snapshot.json', 'r') as f:
        snapshot = json.load(f)
    return snapshot

if __name__ == "__main__":
    start = time.time()
    create_csv_header()

    data_dict = {}
    snapshot = {}
    row_list = {}
     
    rooturl = 'https://www-pwc-com-dpe-staging.pwc.com'  # https://www.pwc.com https://www-pwc-com-dpe-staging.pwc.com
    
    if os.path.exists("data_snapshot.json"):
        print("data_snapshot.json - FOUND!")
        selection = input("Would you like to re-generate snapshot file? Press 1 to regenerate; Press 2 to use current file\n")
        while selection != '1' and selection != '2':
            print('Invalid input')
            selection = input("Would you like to re-generate snapshot? Press 1 to regenerate; Press 2 to continue\n")
        if selection == '1':
            generate_snapshot_file()
            with open("data_snapshot.json", 'w') as f:
                json.dump(snapshot, f, indent=2)  
        elif selection == '2':
            
            calculations(snapshot, row_list, data_dict, rooturl)
            
    
    else:
        selection = input("Would you like to generate a snapshot file from data_snapshot_input.csv? Press 1 to generate; Press 2 to Exit\n")
        while selection != '1' and selection != '2':
            print('Invalid input')
            selection = input("Would you like to generate a snapshot file from data_snapshot_input.csv? Press 1 to generate; Press 2 to continue\n")
        if selection == '1':
            generate_snapshot_file()
        elif selection == '2':
            exit()

    

    #read_data(rooturl)

    end = time.time()
    print(end - start)
    
    print("Done")
    

# https://www.pwc.com/ca/en/industries/entertainment-media/moving-into-multiple-business-models-download-form
#/content/pwc/ca/en/industries/entertainment-media/moving-into-multiple-business-models-download-form